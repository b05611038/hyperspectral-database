import argparse

from hyperspectral_database import HyperspectralDatabase 

def main():
    parser = argparse.ArgumentParser()
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
    parser.add_argument('--transform_batch_size', type = int, default = 20000,
            help = 'The batch size to process transform request.')

    args = parser.parse_args()

    db = HyperspectralDatabase(db_name = args.db_name,
                               user_id = args.user_id,
                               passwd = args.passwd,
                               host = args.host,
                               port = args.port,
                               gridfs = True)

    db.spectral_data_reformation('gridfs', 'list', batch_size = args.transform_batch_size,
            certain = True)

    print('Successfully finsih.')

    return None

if __name__ == '__main__':
    main()
    
