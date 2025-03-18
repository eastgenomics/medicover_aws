from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.schema import MetaData


def connect_to_db(creds: dict):
    """Connect to the postgres database

    Parameters
    ----------
    creds : dict
        Dict containing the credentials needed for connecting to the database

    Returns
    -------
    tuple
        Tuple containing a Session and MetaData objects
    """

    # Create SQLAlchemy engine to connect to AWS database
    url = (
        "postgresql+psycopg2://"
        f"{creds['user']}:{creds['pwd']}@{creds['endpoint']}:{creds['port']}/ngtd"
    )

    engine = create_engine(url)

    meta = MetaData(schema="testdirectory")
    meta.reflect(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    return session, meta


def insert_in_db(session, table, data):
    """Insert the data in the database

    Parameters
    ----------
    session : SQLAlchemy session object
        Session object for the connected database
    table : SQLAlchemy Table object
        Table object in which the data will be imported to
    data : list
        List of dict that need to be imported in the database
    """

    insert_obj = insert(table).values(data)
    session.execute(insert_obj)
    session.commit()
