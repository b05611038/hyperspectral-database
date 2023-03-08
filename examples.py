from hyperspectral_database import HyperspectralDatabase

def acquire_data_examples():
    db = HyperspectralDatabase()
    db.help()
    cursor = db.find({'insert_index': 0})
    cursor = db.find_one({})
    sample_number = db.count_documents({})
    if sample_number > 0:
        data = db.get_data_by_indices([0])
        print('Type of data: {0}'.format(type(data)))
        print('data[0]: ', data[0])
        print('Following functions will retunrn same format.')

        data = db.get_data_by_index_range(1)
        data = db.get_data_by_index_range(0, 1)
        data = db.get_data_by_index_range(0, 1, 1)
        data = db.get_data_by_datatypes(['y-injured-like'])
        data = db.get_data_by_species(['tea12'])

    return None

def main():
    acquire_data_examples()
    print('examples.py finish.')

if __name__ == '__main__':
    main()


