import os
import argparse

from hyperspectral_database import HyperspectralDatabase


def get_data_api_tesing(db):
    data = db.get_data_by_indices([0])
    data = db.get_data_by_index_range(1)
    data = db.get_data_by_index_range(0, 2)
    data = db.get_data_by_index_range(0, 2, 1)
    data = db.get_data_by_datatypes(['y-injured-like'])
    data = db.get_data_by_species(['tea12'])
    data = db.get_all_data()
    return data

def test_API(db, io_testing = False, delete_testing = False, data_path = None):
    if data_path is None:
       raise RuntimeError('Argument: data_path not set.')

    db.help()
    cursor = db.find({'insert_index': 0})
    cursor = db.find_one({})
    sample_number = db.count_documents({})
    if sample_number > 0:
        db.gridfs = True
        db.synchronize_worker = -1
        get_data_api_tesing(db)
        print('Data acquring API (gridfs=True, single-process) testing finish.')

        db.gridfs = True
        db.synchronize_worker = 4
        get_data_api_tesing(db)
        print('Data acquring API (gridfs=True, multi-process) testing finish.')

        db.gridfs = False
        db.synchronize_worker = -1
        get_data_api_tesing(db)
        print('Data acquring API (gridfs=False, single-process) testing finish.')

        db.gridfs = False
        db.synchronize_worker = 4
        get_data_api_tesing(db)
        print('Data acquring API (gridfs=False, multi-process) testing finish.')
    else:
        print('Because no data in database, not testing API of acquring data.')

    if io_testing:
        files = os.listdir(data_path)
        test_file = None
        for f in files:
            if f.endswith('.json'):
                test_file = os.path.join(data_path, f)
                break

        if test_file is None:
            print('Cannot get json file in the data_path, stop testing.')
        else:
            db.insert_data(test_file)
            db.insert_data(test_file, certain = True)
            db.batch_insert_data(data_path, batch_size = 2)
            db.batch_insert_data(data_path, batch_size = 2, certain = True)

        print('IO testing finish.')

    if delete_testing:
        print('delete_testing will delete all file in the database.')
        db.delete_data(0, certain = True)
        db.delete_data([1], certain = True)
        db.delete_all(certain = True)
        print('Delete testing finish.')

    print('testing all finsih.')
    return None

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--data_path', type = str, default = './test_data',
            help = 'The path that store many file.')
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
    parser.add_argument('--query_size', type = int, default = 10000,
            help = 'The partition size of the sycn_wrapper.')
    parser.add_argument('--synchronize_worker', type = int, default = 4,
            help = 'The process number of the multiprocessing.')
    parser.add_argument('--from_list_collection', action = 'store_true',
            help = 'Grab the data from list collection.')
    parser.add_argument('--io_testing', action = 'store_true',
            help = 'Test the IO API of the database object.' + \
            ' Please do not run if you are not developer.')
    parser.add_argument('--delete_testing', action = 'store_true',
            help = 'Test the delete API of the database object.' + \
            ' Please do not run if you are not developer.')

    args = parser.parse_args()

    gridfs = True
    if args.from_list_collection:
        gridfs = False

    db = HyperspectralDatabase(db_name = args.db_name,
                               user_id = args.user_id,
                               passwd = args.passwd,
                               host = args.host,
                               port = args.port,
                               query_size = args.query_size,
                               synchronize_worker = args.synchronize_worker,
                               gridfs = gridfs)

    test_API(db, io_testing = args.io_testing,
                 delete_testing = args.delete_testing,
                 data_path = args.data_path)

    print('Program successfully finish.')

    return None

if __name__ == '__main__':
    main()


