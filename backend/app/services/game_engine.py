from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Team, Checkpoint, Submission, LiveTelemetry, SubmissionStatus, RaceStatus
from app.services import geo_service
from app.services.event_bus import bus


async def get_next_checkpoint(session: AsyncSession, current_id: int | None) -> Checkpoint | None:
    if current_id is None:
        stmt = select(Checkpoint).order_by(Checkpoint.order_index.asc()).limit(1)
    else:
        stmt = (
            select(Checkpoint)
            .where(
                Checkpoint.order_index > select(Checkpoint.order_index)
                .where(Checkpoint.checkpoint_id == current_id)
                .scalar_subquery()
            )
            .order_by(Checkpoint.order_index.asc())
            .limit(1)
        )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def process_location_update(
    session: AsyncSession,
    chat_id: int,
    user_id: int,
    latitude: float,
    longitude: float,
) -> dict:
    team_result = await session.execute(
        select(Team).where(Team.chat_id == chat_id).options(selectinload(Team.current_checkpoint))
    )
    team = team_result.scalar_one_or_none()
    if not team:
        return {"action": "ignore", "reason": "team_not_found"}

    if team.status != RaceStatus.IN_PROGRESS.value:
        return {"action": "ignore", "reason": "race_not_active"}

    telemetry = LiveTelemetry(
        team_id=chat_id,
        user_id=user_id,
        latitude=latitude,
        longitude=longitude,
    )
    session.add(telemetry)

    await bus.publish_telemetry(chat_id, user_id, latitude, longitude)

    if team.current_checkpoint_id is None:
        return {"action": "telemetry_only"}

    checkpoint = team.current_checkpoint
    distance = geo_service.haversine(latitude, longitude, checkpoint.latitude, checkpoint.longitude)

    if geo_service.is_within_radius(latitude, longitude, checkpoint.latitude, checkpoint.longitude, checkpoint.target_radius):
        await session.execute(
            update(Submission)
            .where(
                Submission.team_id == chat_id,
                Submission.checkpoint_id == checkpoint.checkpoint_id,
                Submission.status == SubmissionStatus.PENDING.value,
            )
            .values(status=SubmissionStatus.CORRECT.value)
        )

        next_cp = await get_next_checkpoint(session, checkpoint.checkpoint_id)

        if next_cp is None:
            team.status = RaceStatus.COMPLETED.value
            team.current_checkpoint_id = None
            team.score += 100

            await bus.publish_event("team_completed", {
                "chat_id": chat_id,
                "team_name": team.team_name,
                "final_score": team.score,
            })
            return {"action": "race_complete", "team_name": team.team_name}
        else:
            team.current_checkpoint_id = next_cp.checkpoint_id
            team.score += 50

            await bus.publish_event("checkpoint_unlocked", {
                "chat_id": chat_id,
                "team_name": team.team_name,
                "checkpoint_id": next_cp.checkpoint_id,
                "checkpoint_name": next_cp.name,
            })
            return {
                "action": "checkpoint_unlocked",
                "checkpoint_name": next_cp.name,
                "riddle": next_cp.riddle_text,
                "hint": next_cp.hint,
                "checkpoint_id": next_cp.checkpoint_id,
            }

    return {"action": "telemetry_only", "distance": round(distance, 2), "required_radius": checkpoint.target_radius}


async def process_answer_submission(
    session: AsyncSession,
    chat_id: int,
    user_id: int,
    answer: str,
) -> dict:
    team_result = await session.execute(
        select(Team).where(Team.chat_id == chat_id).options(selectinload(Team.current_checkpoint))
    )
    team = team_result.scalar_one_or_none()
    if not team:
        return {"action": "ignore", "reason": "team_not_found"}

    if team.status != RaceStatus.IN_PROGRESS.value:
        return {"action": "ignore", "reason": "race_not_active"}

    if team.current_checkpoint_id is None:
        return {"action": "ignore", "reason": "no_active_checkpoint"}

    checkpoint = team.current_checkpoint
    submission = Submission(
        team_id=chat_id,
        checkpoint_id=checkpoint.checkpoint_id,
        submitted_answer=answer.strip(),
        submitted_by=user_id,
    )

    if checkpoint.answer and answer.strip().lower() == checkpoint.answer.lower():
        submission.status = SubmissionStatus.CORRECT.value
        session.add(submission)

        next_cp = await get_next_checkpoint(session, checkpoint.checkpoint_id)

        if next_cp is None:
            team.status = RaceStatus.COMPLETED.value
            team.current_checkpoint_id = None
            team.score += 100

            await bus.publish_event("team_completed", {
                "chat_id": chat_id,
                "team_name": team.team_name,
                "final_score": team.score,
            })
            return {"action": "race_complete", "team_name": team.team_name}
        else:
            team.current_checkpoint_id = next_cp.checkpoint_id
            team.score += 50

            await bus.publish_event("checkpoint_unlocked", {
                "chat_id": chat_id,
                "team_name": team.team_name,
                "checkpoint_id": next_cp.checkpoint_id,
                "checkpoint_name": next_cp.name,
            })
            return {
                "action": "correct_answer",
                "checkpoint_name": next_cp.name,
                "riddle": next_cp.riddle_text,
                "hint": next_cp.hint,
                "checkpoint_id": next_cp.checkpoint_id,
            }
    else:
        submission.status = SubmissionStatus.INCORRECT.value
        session.add(submission)
        return {"action": "incorrect_answer", "hint": checkpoint.hint}


async def advance_team_by_checkpoint(
    session: AsyncSession,
    chat_id: int,
    completed_checkpoint_id: int,
) -> dict:
    team_result = await session.execute(select(Team).where(Team.chat_id == chat_id))
    team = team_result.scalar_one_or_none()
    if not team:
        return {"action": "error", "reason": "team_not_found"}

    team.score += 50

    completed_result = await session.execute(
        select(Checkpoint).where(Checkpoint.checkpoint_id == completed_checkpoint_id)
    )
    completed_cp = completed_result.scalar_one_or_none()

    from app.models import Submission
    all_cp_result = await session.execute(select(Checkpoint).order_by(Checkpoint.order_index))
    all_cps = all_cp_result.scalars().all()
    total_count = len(all_cps)

    completed_ids_result = await session.execute(
        select(Submission.checkpoint_id).where(
            Submission.team_id == chat_id,
            Submission.status == "correct",
        )
    )
    completed_ids = {row[0] for row in completed_ids_result.all()}

    if len(completed_ids) >= total_count:
        team.status = RaceStatus.COMPLETED.value
        team.current_checkpoint_id = None
        return {"action": "race_complete", "team_name": team.team_name, "score": team.score}

    remaining_cps = [c for c in all_cps if c.checkpoint_id not in completed_ids]
    next_cp = remaining_cps[0] if remaining_cps else None

    team.current_checkpoint_id = next_cp.checkpoint_id if next_cp else None
    if team.status != RaceStatus.IN_PROGRESS.value:
        team.status = RaceStatus.IN_PROGRESS.value

    return {
        "action": "advanced",
        "team_name": team.team_name,
        "score": team.score,
        "next_checkpoint_name": next_cp.name if next_cp else None,
        "remaining": len(remaining_cps),
    }
