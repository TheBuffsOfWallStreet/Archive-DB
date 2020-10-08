if __name__ == '__main__':
    import db

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
            db.buildIndex()
        elif menu == 'd':
            n = myInput('How Many?:')
            if n.isdigit():
                db.buildEpisodes(int(n))
            elif n == 'all':
                db.buildEpisodes()
            else:
                print(' Invalid Input. Enter number or `all`.')
        elif menu == 'c':
            db.clean()
        elif menu == 'q':
            print(' Goodbye!')
        else:
            print(' Invalid Input')
