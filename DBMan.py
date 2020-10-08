if __name__ == '__main__':
    from db import StoreArchive
    from db import CleanArchive

    def myInput(text):
        return input(f'\033[92m {text} \033[0m')

    menu = None
    while menu != 'q':
        print()
        print('i) Build index')
        print('d) Download episodes')
        print('c) Clean Data')
        print('q) Quit')
        print()

        menu = myInput('Selection:')
        if menu == 'i':
            StoreArchive.buildIndex()
        elif menu == 'd':
            n = myInput('How Many?:')
            if n.isdigit():
                StoreArchive.buildEpisodes(int(n))
            elif n == 'all':
                StoreArchive.buildEpisodes()
            else:
                print(' Invalid Input. Enter number or `all`.')
        elif menu == 'c':
            CleanArchive.clean()
        elif menu == 'q':
            print(' Goodbye!')
        else:
            print(' Invalid Input')
