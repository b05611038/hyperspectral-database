import os
import argparse

from hyperspectral_database import HyperspectralDatabase

def test_API(io_testing = False, delete_testing = False, data_path = None):
    if data_path is None:
       raise RuntimeError('Argument: data_path not set.')

    db = HyperspectralDatabase()
    db.help()
    cursor = db.find({'insert_index': 0})
    cursor = db.find_one({})
    sample_number = db.count_documents({})
    if sample_number > 0:
        data = db.get_data_by_indices([0])
        data = db.get_data_by_index_range(1)
        data = db.get_data_by_index_range(0, 1)
        data = db.get_data_by_index_range(0, 1, 1)
        data = db.get_data_by_datatypes(['y-injured-like'])
        data = db.get_data_by_species(['tea12'])
        data = db.get_all_data()
        print('Data acquring API testing finish.')
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
    parser.add_argument('--io_testing', action = 'store_true',
            help = 'Test the IO API of the database object.' + \
            ' Please do not run if you are not developer.')
    parser.add_argument('--delete_testing', action = 'store_true',
            help = 'Test the delete API of the database object.' + \
            ' Please do not run if you are not developer.')

    args = parser.parse_args()
    test_API(io_testing = args.io_testing,
             delete_testing = args.delete_testing,
             data_path = args.data_path)

    print('Program successfully finish.')
    return None

if __name__ == '__main__':
    main()


