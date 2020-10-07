if __name__ == '__main__':
    from db import StoreArchive

    menu = None
    while menu != 'q':
        print()
        print('i) Build index')
        print('d) Download episodes')
        print('q) Quit')

        menu = input('\n Selection:')
        if menu == 'i':
            StoreArchive.buildIndex()
        elif menu == 'd':
            StoreArchive.buildEpisodes()
        else:
            print(' Invalid Input')
