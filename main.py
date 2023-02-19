import asyncio
import datetime
from aiohttp import ClientSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, JSON, String, Text
from more_itertools import chunked


PG_DSN = 'postgresql+asyncpg://app:secret@127.0.0.1:5431/app'
engine = create_async_engine(PG_DSN)
Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String(30))
    eye_color = Column(String(30))
    films = Column(Text)
    gender = Column(String(30))
    hair_color = Column(String(30))
    height = Column(String(30))
    homeworld = Column(String(60))
    mass = Column(String(30))
    name = Column(String(60))
    skin_color = Column(String(60))
    species = Column(Text)
    starships = Column(Text)
    vehicles = Column(Text)


CHUNK_SIZE = 10
PEOPLES_COUNT = 90
cash = dict()


async def chunked_async(async_iter, size):

    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            if buffer:
                yield buffer
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_person(people_id: int, session: ClientSession):
    print(f'begin {people_id}')
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()

    film_titles = []
    if 'films' in json_data:
        for film in json_data["films"]:
            title = await get_title(film, session)
            film_titles.append(title)
    json_data["film_titles"] = ','.join(film_titles)

    species_names = []
    if 'species' in json_data:
        for specy in json_data["species"]:
            name = await get_name(specy, session)
            species_names.append(name)
    json_data["species_names"] = ','.join(species_names)

    starships_names = []
    if 'starships' in json_data:
        for starship in json_data["starships"]:
            name = await get_name(starship, session)
            starships_names.append(name)
    json_data["starships_names"] = ','.join(starships_names)

    vehicles_names = []
    if 'vehicles' in json_data:
        for vehicle in json_data["vehicles"]:
            name = await get_name(vehicle, session)
            vehicles_names.append(name)
    json_data["vehicles_names"] = ','.join(vehicles_names)

    json_data["id"] = people_id

    print(f'end {people_id}')
    return json_data


async def get_title(url: str, session: ClientSession):
    if url in cash:
        return cash[url]
    print(f'begin {url}')
    async with session.get(url) as response:
        json_data = await response.json()
    cash[url] = json_data['title']
    print(f'end {url}')
    return json_data['title']


async def get_name(url: str, session: ClientSession):
    if url in cash:
        return cash[url]
    print(f'begin {url}')
    async with session.get(url) as response:
        json_data = await response.json()
    cash[url] = json_data['name']
    print(f'end {url}')
    return json_data['name']


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, PEOPLES_COUNT), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)
            for item in results:
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all([People(id=item['id'],
        birth_year=item.get('birth_year', ''),
        eye_color=item.get('eye_color', ''),
        films=item['film_titles'],
        gender=item.get('gender', ''),
        hair_color=item.get('hair_color', ''),
        height=item.get('height', ''),
        homeworld=item.get('homeworld', ''),
        mass=item.get('mass', ''),
        name=item.get('name', ''),
        skin_color=item.get('skin_color', ''),
        species=item['species_names'],
        starships=item['starships_names'],
        vehicles=item['vehicles_names'],) for item in people_chunk if 'name' in item])
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        asyncio.create_task(insert_people(chunk))

    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task
    

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
