from sqlalchemy import Column, Integer, String, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


DSN = 'postgresql://app:secret@127.0.0.1:5431/app'

engine = create_engine(DSN)

Base = declarative_base()
Session = sessionmaker(bind=engine)


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


with Session() as session:
    q = session.query(People.name, People.birth_year).all()
    for i in q:
        print(*i)
