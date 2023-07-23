from utils import *


def main():
    employers_id_list = [
        '3529', '77364', '54979', '1942330', '103816', '60377',
        '104309', '39305', '56478', '30151'
    ]
    database = choose_bd(employers_id_list)

    menu(database)


if __name__ == '__main__':
    main()