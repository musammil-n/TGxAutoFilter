
from pyrogram import Client, filters
import datetime
import time
from database.users_chats_db import db
from info import ADMINS
from utils import broadcast_messages
import asyncio

BROADCAST_BATCH_SIZE = 500  # Now processes 500 users at a time
BROADCAST_SLEEP = 1  # Small delay to avoid rate limits

@Client.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
async def broadcast(bot, message):
    users = await db.get_all_users()
    b_msg = message.reply_to_message
    sts = await message.reply_text("Broadcasting your messages...")
    
    start_time = time.time()
    total_users = await db.total_users_count()
    done, blocked, deleted, failed, success = 0, 0, 0, 0, 0

    async def send_message(user):
        nonlocal success, blocked, deleted, failed
        user_id = int(user['id'])
        pti, sh = await broadcast_messages(user_id, b_msg)

        if pti:
            success += 1
        else:
            if sh == "Blocked":
                blocked += 1
                await db.delete_user(user_id)  # Remove blocked user
            elif sh == "Deleted":
                deleted += 1
                await db.delete_user(user_id)  # Remove deleted user
            elif sh == "Error":
                failed += 1

    tasks = []
    async for user in users:
        tasks.append(send_message(user))
        done += 1

        # Process messages in batches of 500
        if len(tasks) >= BROADCAST_BATCH_SIZE:
            await asyncio.gather(*tasks)
            tasks = []
            await sts.edit(
                f"Broadcast in progress:\n\nTotal Users: {total_users}\nCompleted: {done} / {total_users}\n"
                f"Success: {success} | Blocked: {blocked} | Deleted: {deleted} | Failed: {failed}"
            )
            await asyncio.sleep(BROADCAST_SLEEP)  # Small delay to avoid rate limits

    # Process remaining messages (if any)
    if tasks:
        await asyncio.gather(*tasks)

    time_taken = datetime.timedelta(seconds=int(time.time() - start_time))
    await sts.edit(
        f"Broadcast Completed in {time_taken}.\n\nTotal Users: {total_users}\n"
        f"Success: {success} | Blocked: {blocked} | Deleted: {deleted} | Failed: {failed}"
    )
    
