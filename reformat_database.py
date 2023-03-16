from hyperspectral_database import HyperspectralDatabase 

def main():
    db = HyperspectralDatabase(host = '127.0.0.1', 
            port = 27087, query_size = 10000, synchronize_worker = 4)

    db.spectral_data_reformation('gridfs', 'list', certain = True)
    print('Successfully finsih.')
    return None

if __name__ == '__main__':
    main()
    
