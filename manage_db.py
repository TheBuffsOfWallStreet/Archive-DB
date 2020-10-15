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
            print('Suggested networks:')
            print('b) BLOOMBERG')
            print('c) CNBC')
            print('f) FOX BUSINESS')
            print('o) Other')
            network_choice = myInput('Network?:')
            if network_choice == 'b':
                network = 'BLOOMBERG'
            elif network_choice == 'f':
                network = 'FBC'
            elif network_choice == 'c':
                network = 'CNBC'
            elif network_choice == 'o':
                network = myInput('Other Network?:')
            else:
                print('Invalid Input')
                continue
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
