import logging
import os
import asyncio
import threading
import json
import uuid
from pathlib import Path
from sqlalchemy import select

from fastapi import APIRouter, Request

from app.database import get_db
from app.services.game_engine import process_location_update, process_answer_submission, advance_team_by_checkpoint
from app.services.vision_service import identify_checkpoint
from app.services.telegram_service import (
    send_message, send_checkpoint_riddle,
    send_congratulation, send_race_complete, send_wrong_answer,
)
from app.services.event_bus import bus

logger = logging.getLogger(__name__)
router = APIRouter(tags=["webhook"])

HQ_CHAT_ID = -5173862362
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SUBMISSIONS_DIR = PROJECT_ROOT / "submissions"

pending_reviews: dict[str, dict] = {}


async def _forward_to_hq(bot, chat_id: int, message_data: dict, team_name: str, caption: str = "", reply_markup=None):
    try:
        if "photo" in message_data:
            photo_sizes = message_data["photo"]
            largest = max(photo_sizes, key=lambda p: p.get("file_size", 0))
            await bot.send_photo(
                chat_id=HQ_CHAT_ID,
                photo=largest["file_id"],
                caption=f"<b>{team_name}</b>\n{caption}",
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
        elif "video" in message_data:
            video = message_data["video"]
            await bot.send_video(
                chat_id=HQ_CHAT_ID,
                video=video["file_id"],
                caption=f"<b>{team_name}</b>\n{caption}",
                parse_mode="HTML",
                supports_streaming=True,
                reply_markup=reply_markup,
            )
    except Exception as e:
        logger.error(f"Failed to forward to HQ: {e}")


def _upload_to_gdrive(file_bytes: bytes, filename: str, mime_type: str):
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseUpload
        from io import BytesIO
        from app.config import get_settings

        s = get_settings()

        creds = Credentials.from_authorized_user_info(
            {
                "client_id": s.gdrive_client_id,
                "client_secret": s.gdrive_client_secret,
                "refresh_token": s.gdrive_refresh_token,
                "token_uri": "https://oauth2.googleapis.com/token",
                "scopes": ["https://www.googleapis.com/auth/drive"],
            },
        )

        drive = build("drive", "v3", credentials=creds, cache_discovery=False)

        from app.config import get_settings
        s = get_settings()

        folder_id = s.gdrive_folder_id
        if not folder_id:
            logger.warning("GDRIVE_FOLDER_ID not set, skipping upload")
            return

        fh = BytesIO(file_bytes)
        media = MediaIoBaseUpload(fh, mimetype=mime_type, resumable=True)
        body = {"name": filename, "parents": [folder_id]}

        def _upload():
            try:
                drive.files().create(body=body, media_body=media, fields="id").execute()
                logger.info(f"Uploaded {filename} to GDrive")
            except Exception as e:
                logger.error(f"GDrive upload failed: {e}")

        thread = threading.Thread(target=_upload, daemon=True)
        thread.start()
    except Exception as e:
        logger.error(f"GDrive upload error: {e}")


def _get_mime(filename: str) -> str:
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
    return {
        "jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
        "mp4": "video/mp4", "mov": "video/quicktime", "avi": "video/x-msvideo",
    }.get(ext, "application/octet-stream")


async def _get_checkpoint_buttons():
    from app.models import Checkpoint
    from app.database import async_session
    async with async_session() as db:
        result = await db.execute(select(Checkpoint).order_by(Checkpoint.order_index))
        return [(cp.checkpoint_id, cp.name) for cp in result.scalars().all()]


def _build_approval_keyboard(review_id: str, checkpoint_buttons: list[tuple] = None) -> dict:
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    rows = [
        [
            InlineKeyboardButton(text="Approve", callback_data=f"approve_{review_id}"),
            InlineKeyboardButton(text="Reject", callback_data=f"reject_{review_id}"),
        ]
    ]
    if checkpoint_buttons:
        cp_rows = []
        for cp_id, cp_name in checkpoint_buttons:
            short_name = cp_name[:30]
            cp_rows.append(InlineKeyboardButton(
                text=short_name, callback_data=f"cp_{review_id}_{cp_id}"
            ))
        for i in range(0, len(cp_rows), 2):
            rows.append(cp_rows[i:i+2])
    keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
    return keyboard


def _build_checkpoint_keyboard(review_id: str, checkpoint_buttons: list[tuple]) -> dict:
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    rows = [[InlineKeyboardButton(
        text="Start", callback_data=f"assign_{review_id}_0"
    )]]
    for cp_id, cp_name in checkpoint_buttons:
        short_name = cp_name[:30]
        rows.append(InlineKeyboardButton(
            text=short_name, callback_data=f"assign_{review_id}_{cp_id}"
        ))
    chunked = [rows[0]] + [rows[i:i+2] for i in range(1, len(rows))]
    keyboard = InlineKeyboardMarkup(inline_keyboard=chunked)
    return keyboard


@router.post("/webhook/telegram")
async def handle_telegram_webhook(request: Request):
    payload = await request.json()

    if "callback_query" in payload:
        cq = payload["callback_query"]
        if cq:
            logger.info(f"GOT CALLBACK: {cq.get('data','')}")
            try:
                await _handle_callback(cq)
            except Exception as e:
                logger.error(f"Callback error: {e}", exc_info=True)
        return {"status": "ok"}

    message_data = payload.get("edited_message") or payload.get("message")
    if not message_data:
        return {"status": "ignored", "reason": "no_message"}

    chat_info = message_data.get("chat", {})
    chat_id = chat_info.get("id")
    chat_type = chat_info.get("type", "")

    from_info = message_data.get("from", {})
    user_id = from_info.get("id")
    username = from_info.get("username", "")
    text_preview = (message_data.get("text") or "")[:50]

    logger.info(f"MSG chat={chat_id} user={user_id} uname={username} text={text_preview}")

    if chat_type not in ("group", "supergroup"):
        return {"status": "ignored", "reason": "not_group"}

    if "photo" in message_data or "video" in message_data:
        try:
            await _handle_media(chat_id, user_id, message_data)
        except Exception as e:
            logger.error(f"Media handling error: {e}", exc_info=True)
    elif "location" in message_data:
        location = message_data["location"]
        lat = location["latitude"]
        lon = location["longitude"]

        async for db in get_db():
            result = await process_location_update(db, chat_id, user_id, lat, lon)

        if result["action"] == "checkpoint_unlocked":
            await send_congratulation(chat_id, result["checkpoint_name"])
            await send_checkpoint_riddle(
                chat_id, result["checkpoint_name"], result.get("riddle"), result.get("hint")
            )
        elif result["action"] == "race_complete":
            await send_congratulation(chat_id, "Final Checkpoint")
            await send_race_complete(chat_id, result.get("final_score", 0))

    elif "text" in message_data and "reply_to_message" not in message_data:
        text = message_data["text"].strip()

        if text.startswith("/"):
            await _handle_command(chat_id, user_id, text)
        else:
            async for db in get_db():
                result = await process_answer_submission(db, chat_id, user_id, text)

            if result["action"] == "correct_answer":
                await send_congratulation(chat_id, "Correct Answer!")
                await send_checkpoint_riddle(
                    chat_id, result["checkpoint_name"], result.get("riddle"), result.get("hint")
                )
            elif result["action"] == "incorrect_answer":
                await send_wrong_answer(chat_id, result.get("hint"))
            elif result["action"] == "race_complete":
                await send_congratulation(chat_id, "Final Checkpoint")
                await send_race_complete(chat_id, result.get("final_score", 0))

    return {"status": "ok"}


async def _handle_callback(callback_query: dict):
    from aiogram import Bot
    from app.config import get_settings

    callback_data = callback_query.get("data", "")
    callback_id = callback_query.get("id", "")
    msg = callback_query.get("message") or {}
    from_chat_id = msg.get("chat", {}).get("id")

    logger.info(f"CALLBACK data={callback_data} from_chat={from_chat_id} hq={HQ_CHAT_ID}")

    settings = get_settings()
    bot = Bot(token=settings.telegram_bot_token)

    try:
        await bot.answer_callback_query(callback_id)
    except Exception:
        pass

    if from_chat_id != HQ_CHAT_ID:
        logger.warning(f"Callback from non-HQ chat {from_chat_id}")
        await bot.session.close()
        return

    if callback_data.startswith("approve_"):
        review_id = callback_data[len("approve_"):]
        review = pending_reviews.get(review_id)
        if not review:
            logger.warning(f"Approve: review {review_id} not found (keys: {list(pending_reviews.keys())})")
            await bot.session.close()
            return

        cp_buttons = await _get_checkpoint_buttons()
        keyboard = _build_checkpoint_keyboard(review_id, cp_buttons)

        team_name = review["team_name"]
        try:
            await bot.edit_message_caption(
                chat_id=HQ_CHAT_ID,
                message_id=callback_query.get("message", {}).get("message_id"),
                caption=f"<b>{team_name}</b>\n\nWhich checkpoint?",
                parse_mode="HTML",
                reply_markup=keyboard,
            )
        except Exception as e:
            logger.error(f"Failed to edit HQ message: {e}")

    elif callback_data.startswith("assign_"):
        parts = callback_data.split("_", 2)
        review_id = parts[1]
        checkpoint_id = int(parts[2])

        review = pending_reviews.pop(review_id, None)
        if not review:
            logger.warning(f"Assign: review {review_id} not found")
            await bot.session.close()
            return

        review["checkpoint_id"] = checkpoint_id
        team_name = review["team_name"]
        await _handle_hq_approved(review, bot)

        try:
            await bot.edit_message_caption(
                chat_id=HQ_CHAT_ID,
                message_id=callback_query.get("message", {}).get("message_id"),
                caption=f"<b>{team_name}</b>\n\nInformed Team {team_name}",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"Failed to edit HQ message: {e}")

    elif callback_data.startswith("reject_"):
        review_id = callback_data[len("reject_"):]
        review = pending_reviews.pop(review_id, None)
        if not review:
            logger.warning(f"Reject: review {review_id} not found")
        else:
            chat_id = review["chat_id"]
            team_name = review["team_name"]
            await send_message(chat_id, "That's not the right checkpoint. Try again!")
            try:
                await bot.edit_message_caption(
                    chat_id=HQ_CHAT_ID,
                    message_id=callback_query.get("message", {}).get("message_id"),
                    caption=f"<b>{team_name}</b>\n\nRejected for Team {team_name}",
                    parse_mode="HTML",
                )
            except Exception as e:
                logger.error(f"Failed to edit HQ message: {e}")
    else:
        logger.warning(f"Unknown callback data: {callback_data}")

    await bot.session.close()


async def _handle_hq_approved(review: dict, bot):
    from app.models import Team, Checkpoint, Submission
    from app.database import async_session

    chat_id = review["chat_id"]
    team_name = review["team_name"]
    user_id = review.get("user_id")
    checkpoint_id = review.get("checkpoint_id")

    async with async_session() as db:
        if not checkpoint_id:
            await send_message(chat_id, "Your submission has been approved by HQ!")
            await bus.publish_event("checkpoint_unlocked", {
                "chat_id": chat_id, "team_name": team_name,
                "checkpoint_name": "Approved submission",
            })
            return

        if checkpoint_id == 0:
            from app.models import Team, RaceStatus
            team_result = await db.execute(select(Team).where(Team.chat_id == chat_id))
            team = team_result.scalar_one_or_none()
            if team:
                team.status = RaceStatus.IN_PROGRESS.value
            await db.commit()
            await bus.publish_event("checkpoint_unlocked", {
                "chat_id": chat_id, "team_name": team_name,
                "checkpoint_name": "Start",
            })
            await send_message(chat_id, "Your submission has been approved! The race has begun!\n\nHead to the first checkpoint!")
            return

        cp_result = await db.execute(
            select(Checkpoint).where(Checkpoint.checkpoint_id == checkpoint_id)
        )
        cp = cp_result.scalar_one_or_none()
        checkpoint_name = cp.name if cp else f"Checkpoint {checkpoint_id}"

        submission = Submission(
            team_id=chat_id,
            checkpoint_id=checkpoint_id,
            submitted_by=user_id,
            status="correct",
            image_path=review.get("image_path", ""),
        )
        db.add(submission)

        result = await advance_team_by_checkpoint(db, chat_id, checkpoint_id)
        await db.commit()

        cp_all = await db.execute(select(Checkpoint).order_by(Checkpoint.order_index))
        all_cps = cp_all.scalars().all()

        completed_result = await db.execute(
            select(Submission.checkpoint_id).where(
                Submission.team_id == chat_id,
                Submission.status == "correct",
            )
        )
        completed_ids = {row[0] for row in completed_result.all()}

        remaining_cps = [c for c in all_cps if c.checkpoint_id not in completed_ids]
        options_text = ""
        if remaining_cps:
            options_text = "\n\n<b>Remaining checkpoints:</b>\n" + "\n".join(
                f"  {i+1}. {c.name}" for i, c in enumerate(remaining_cps)
            )

        await bus.publish_event("checkpoint_unlocked", {
            "chat_id": chat_id, "team_name": team_name,
            "checkpoint_name": checkpoint_name,
        })

        if result["action"] == "race_complete":
            await send_message(chat_id, f"Checkpoint <b>{checkpoint_name}</b> submitted!!!!\n\nYou've completed the race!")
        else:
            await send_message(chat_id, f"You just cleared: <b>{checkpoint_name}</b>\n\nHead to the next checkpoint!" + options_text)


async def _handle_media(chat_id: int, user_id: int, message_data: dict):
    from app.models import Team, Checkpoint, Submission, RaceStatus
    from app.database import async_session
    from aiogram import Bot
    from app.config import get_settings
    from datetime import datetime

    settings = get_settings()
    bot = Bot(token=settings.telegram_bot_token)

    try:
        if "video" in message_data:
            video = message_data["video"]
            file_id = video["file_id"]
            ext = "mp4"
            is_video = True
        else:
            photo_sizes = message_data["photo"]
            largest = max(photo_sizes, key=lambda p: p.get("file_size", 0))
            file_id = largest["file_id"]
            ext = "jpg"
            is_video = False

        file = await bot.get_file(file_id)
        downloaded = await bot.download_file(file.file_path)
        file_bytes = downloaded.read()
    except Exception as e:
        logger.error(f"Failed to download media: {e}")
        await send_message(chat_id, "Sorry, I couldn't download your media. Please try again.")
        await bot.session.close()
        return

    async with async_session() as db:
        team_result = await db.execute(select(Team).where(Team.chat_id == chat_id))
        team = team_result.scalar_one_or_none()

        team_name = team.team_name if team else f"Unknown_{chat_id}"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        safe_team = team_name.replace(" ", "_").replace("/", "_").replace("@", "_").replace("&", "_")
        base_filename = f"{safe_team}_{timestamp}"

        submissions_dir = SUBMISSIONS_DIR / str(abs(chat_id))
        submissions_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{base_filename}.{ext}"
        filepath = submissions_dir / filename
        filepath.write_bytes(file_bytes)

        gdrive_filename = filename
        threading.Thread(target=_upload_to_gdrive, args=(file_bytes, gdrive_filename, _get_mime(filename)), daemon=True).start()

        image_path = str(filepath.relative_to(PROJECT_ROOT))

        if not is_video:
            cp_result = await db.execute(select(Checkpoint).order_by(Checkpoint.order_index))
            all_checkpoints = cp_result.scalars().all()
            cp_dicts = [
                {"checkpoint_id": cp.checkpoint_id, "name": cp.name,
                 "task_description": cp.task_description, "order_index": cp.order_index}
                for cp in all_checkpoints
            ]

            await send_message(chat_id, "Analyzing your photo...")
            matched = await identify_checkpoint(file_bytes, cp_dicts)

            if matched:
                submission = Submission(
                    team_id=chat_id,
                    checkpoint_id=matched["checkpoint_id"],
                    submitted_by=user_id,
                    status="correct",
                    image_path=image_path,
                )
                db.add(submission)

                result = await advance_team_by_checkpoint(db, chat_id, matched["checkpoint_id"])
                await db.commit()

                checkpoint_name = matched["name"]

                completed_result = await db.execute(
                    select(Submission.checkpoint_id).where(
                        Submission.team_id == chat_id,
                        Submission.status == "correct",
                    )
                )
                completed_ids = {row[0] for row in completed_result.all()}

                remaining_cps = [cp for cp in cp_dicts if cp["checkpoint_id"] not in completed_ids]
                options_text = ""
                if remaining_cps:
                    options_text = "\n\n<b>Remaining checkpoints:</b>\n" + "\n".join(
                        f"  {i+1}. {cp['name']}" for i, cp in enumerate(remaining_cps)
                    )

                await _forward_to_hq(bot, chat_id, message_data, team_name,
                                     f"AUTO-APPROVED: {checkpoint_name}")

                await bus.publish_event("checkpoint_unlocked", {
                    "chat_id": chat_id, "team_name": team_name,
                    "checkpoint_name": checkpoint_name,
                })

                if result["action"] == "race_complete":
                    await send_message(chat_id, f"Checkpoint <b>{checkpoint_name}</b> submitted!!!!\n\nYou've completed the race!")
                else:
                    await send_message(chat_id, f"You just cleared: <b>{checkpoint_name}</b>\n\nHead to the next checkpoint!" + options_text)
            else:
                review_id = str(uuid.uuid4())[:8]
                pending_reviews[review_id] = {
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "team_name": team_name,
                    "checkpoint_name": None,
                    "ai_suggestion": None,
                    "image_path": image_path,
                    "is_video": False,
                }

                keyboard = _build_approval_keyboard(review_id)
                await _forward_to_hq(
                    bot, chat_id, message_data, team_name,
                    "AI could not identify. Approve or Reject?",
                    reply_markup=keyboard,
                )

                await send_message(chat_id, "Photo received! Waiting for HQ to verify...")
        else:
            review_id = str(uuid.uuid4())[:8]
            pending_reviews[review_id] = {
                "chat_id": chat_id,
                "user_id": user_id,
                "team_name": team_name,
                "checkpoint_name": None,
                "ai_suggestion": None,
                "image_path": image_path,
                "is_video": True,
            }

            keyboard = _build_approval_keyboard(review_id)
            await _forward_to_hq(
                bot, chat_id, message_data, team_name,
                "Video submission. Approve or Reject?",
                reply_markup=keyboard,
            )

            await send_message(chat_id, "Video received! Waiting for HQ to verify...")

    await bot.session.close()


async def _handle_command(chat_id: int, user_id: int, text: str) -> None:
    parts = text.split(maxsplit=2)
    cmd = parts[0].lower()

    if cmd == "/join":
        display_name = parts[1] if len(parts) > 1 else f"Runner_{user_id}"
        from app.models import Team, TeamMember
        from app.database import async_session

        async with async_session() as db:
            result = await db.execute(select(Team).where(Team.chat_id == chat_id))
            team = result.scalar_one_or_none()
            if not team:
                team = Team(chat_id=chat_id, team_name=f"Team_{chat_id}")
                db.add(team)

            member = TeamMember(user_id=user_id, chat_id=chat_id, display_name=display_name)
            db.add(member)
            await db.commit()

        await send_message(chat_id, f"Welcome <b>{display_name}</b>! You've joined the race.")

    elif cmd == "/status":
        from app.models import Team, Submission
        from app.database import async_session

        async with async_session() as db:
            result = await db.execute(select(Team).where(Team.chat_id == chat_id))
            team = result.scalar_one_or_none()
            if team:
                sub_result = await db.execute(
                    select(Submission.checkpoint_id).where(
                        Submission.team_id == chat_id, Submission.status == "correct",
                    )
                )
                completed = len(sub_result.scalars().all())
                status_msg = (
                    f"<b>Team:</b> {team.team_name}\n"
                    f"<b>Status:</b> {team.status}\n"
                    f"<b>Score:</b> {team.score} pts\n"
                    f"<b>Checkpoints:</b> {completed}/12"
                )
            else:
                status_msg = "No team registered for this group. Use /join to register."
        await send_message(chat_id, status_msg)

    elif cmd == "/checkpoints":
        from app.models import Checkpoint
        from app.database import async_session

        async with async_session() as db:
            result = await db.execute(select(Checkpoint).order_by(Checkpoint.order_index))
            checkpoints = result.scalars().all()

        lines = ["<b>Checkpoints:</b>\n"]
        for i, cp in enumerate(checkpoints, 1):
            lines.append(f"{i}. <b>{cp.name}</b>")
            if cp.task_description:
                lines.append(f"   {cp.task_description}")
        await send_message(chat_id, "\n".join(lines))

    elif cmd == "/hint":
        from app.models import Team, Checkpoint
        from app.database import async_session

        async with async_session() as db:
            result = await db.execute(select(Team).where(Team.chat_id == chat_id))
            team = result.scalar_one_or_none()
            if team and team.current_checkpoint_id:
                cp_result = await db.execute(
                    select(Checkpoint).where(Checkpoint.checkpoint_id == team.current_checkpoint_id)
                )
                cp = cp_result.scalar_one_or_none()
                if cp and cp.hint:
                    await send_message(chat_id, f"<i>Hint:</i> {cp.hint}")
                    return
        await send_message(chat_id, "No hint available right now.")
