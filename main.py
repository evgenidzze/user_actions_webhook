import asyncio
import websockets
import aiomysql
import json
from datetime import datetime


async def handle_websocket(websocket, path):
    db_pool = await aiomysql.create_pool(
        host='130.0.238.226',
        user='Zhenya',
        password='AaLaBu14!W',
        db='Signals_avi',
        autocommit=True
    )

    try:
        async with db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                last_id = None

                async with db_pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cursor:
                        while True:
                            if last_id:
                                await cursor.execute(f"SELECT * FROM user_actions WHERE id > '{last_id}'")
                            else:
                                await cursor.execute("SELECT * FROM user_actions")

                            result = await cursor.fetchall()

                            if result:
                                for row in result:
                                    row['action_time'] = row['action_time'].strftime('%m-%d %H:%M')

                                await websocket.send(json.dumps(result))
                                last_id = result[-1].get('id')

                            await asyncio.sleep(10)
    except websockets.exceptions.ConnectionClosedOK:
        print("Connection closed by the client.")
    finally:
        db_pool.close()
        await db_pool.wait_closed()


if __name__ == "__main__":
    start_server = websockets.serve(handle_websocket, "localhost", 8765)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()
