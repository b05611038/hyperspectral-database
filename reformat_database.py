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
    parser.add_argument('--query_size', type = int, default = 1000,
            help = 'The partition size of the sycn_wrapper.')
    parser.add_argument('--transform_batch_size', type = int, default = 20000,
            help = 'The batch size to process transform request.')
    parser.add_argument('--synchronize_worker', type = int, default = 5,
            help = 'The process number of the multiprocessing.')

    args = parser.parse_args()

    db = HyperspectralDatabase(db_name = args.db_name,
                               user_id = args.user_id,
                               passwd = args.passwd,
                               host = args.host,
                               port = args.port,
                               query_size = args.query_size,
                               synchronize_worker = args.synchronize_worker,
                               gridfs = True)

    db.spectral_data_reformation('gridfs', 'list', batch_size = args.transform_batch_size,
            certain = True)
    print('Successfully finsih.')
    return None

if __name__ == '__main__':
    main()
    
