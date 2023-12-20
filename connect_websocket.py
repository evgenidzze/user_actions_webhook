import asyncio
import websockets
import json

from utils import insert_user_data_excel

category_num = {'START': 1, 'BOTtni bepul olish / Entrar no VIP': 5, 'Faollashtiring / Juntar-se': 9,
                'Bot qanday ishlaydi?! / Como é que funciona?!': 13, 'Otzivlar / Feedback': 17,
                'tez-tez / Porquê seguir': 21, 'Davom etish / Continuar': 25,
                'Qanday qilib ro\'yxatdan / Como registar?': 29, 'Depozitni qanday / Como depositar?': 33}


async def receive_updates():
    uri = "ws://localhost:8765"
    async with websockets.connect(uri, max_size=None) as websocket:
        while True:
            message = await websocket.recv()
            user_categories = {}
            new_data: list = json.loads(message)
            if len(new_data) < 20000:
                for user in new_data:
                    if user_categories.get(user.get('stage')):
                        user_categories.get(user.get('stage')).append(user)
                    else:
                        user_categories[user.get('stage')] = []
                        user_categories[user.get('stage')].append(user)

                for stage, user_list in user_categories.items():
                    x = category_num.get(stage)
                    print(f'found {x} by key {stage}')
                    if x and user_list:
                        insert_user_data_excel(users=user_list, x=category_num[stage])


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(receive_updates())
