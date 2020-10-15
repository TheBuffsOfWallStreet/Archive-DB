if __name__ == '__main__':
    import Database

    def myInput(text):
        return input(f'\033[92m {text} \033[0m')

    menu = None
    while menu != 'q':
        print()
        print('i) Build index')
        print('d) Download episodes')
        print('c) Clean Data')
        print('v) Create Views')
        print('q) Quit')
        print()

        menu = myInput('Selection:')
        if menu == 'i':
            print('Suggested networks: BLOOMBERG, FBC, CNBC')
            network = myInput('Network?:')
            Database.buildIndex(network)
        elif menu == 'd':
            n = myInput('How Many?:')
            if n.isdigit():
                Database.buildEpisodes(int(n))
            elif n == 'all':
                Database.buildEpisodes()
            else:
                print(' Invalid Input. Enter number or `all`.')
        elif menu == 'c':
            all = myInput('Clean All? (y/n):')
            if all == 'y':
                Database.clean()
            elif all == 'n':
                Database.clean(all=False)
        elif menu == 'v':
            Database.createViews()
        elif menu == 'q':
            print(' Goodbye!')
        else:
            print(' Invalid Input')
