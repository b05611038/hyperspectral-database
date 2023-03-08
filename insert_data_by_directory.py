import os
import argparse

from hyperspectral_database import HyperspectralDatabase

def insert_data_by_directory(db, directory):
    db.batch_insert_data(directory, one_by_one_insert = True, progress = True, certain = True)
    print('\nInsertion finish.')
    return None

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('directory', type = str,
            help = 'The directory that you store file')

    parser.add_argument('--db_name', type = str, default = 'hyperspectral',
            help = 'The database name of the deployed MongoDB.')
    parser.add_argument('--user_id', type = str, default = '',
            help = 'The user name of the deployed MongoDB.')
    parser.add_argument('--passwd', type = str, default = '',
            help = 'The password of the deployed MongoDB.')
    parser.add_argument('--host', type = str, default = '192.168.50.146',
            help = 'The host of the deployed MongoDB.')
    parser.add_argument('--port', type = int, default = 27087,
            help = 'The port of the deployed MongoDB.')

    args = parser.parse_args()
    db = HyperspectralDatabase(db_name = args.db_name,
                               user_id = args.user_id,
                               passwd = args.passwd,
                               host = args.host,
                               port = args.port)

    insert_data_by_directory(db, args.directory)
    print('Program finish.')

    return None

if __name__ == '__main__':
    main()


