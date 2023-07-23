from database import DataBase


class DBManager:
    '''
        Класс для работы с данными базы данных
        '''

    def __init__(self, database: DataBase) -> None:
        self.database = database
        self.connection = database.connection

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({type(self.database)})'

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании."""

        connection = self.connection
        data_list = []

        with connection.cursor() as cursor:
            cursor.execute('SELECT name, open_vacancies FROM employers')
            data_list.extend(cursor.fetchall())

        return data_list

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""

        connection = self.connection
        vacancy_list = []

        with connection.cursor() as cursor:
            cursor.execute('''
                SELECT employers.name, vacancies.name, vacancies.salary, vacancies.currency, vacancies.url
                FROM vacancies
                JOIN employers USING(employer_id) 
            '''
                           )
            vacancy_list.extend(cursor.fetchall())

        return vacancy_list

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""

        connection = self.connection

        with connection.cursor() as cursor:
            cursor.execute('''
                SELECT ROUND(AVG(salary))
                FROM vacancies
                WHERE salary IS NOT NULL
            ''')

            return cursor.fetchall()[0][0]

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""

        connection = self.connection
        avg_salary = self.get_avg_salary()
        vacancy_list = []

        with connection.cursor() as cursor:
            cursor.execute('''
                SELECT name, description, area, salary_from, salary_to, salary,
                       currency, experience, employment, address, url
                FROM vacancies
                WHERE salary IS NOT NULL AND salary > %s
            ''', (avg_salary,))

            vacancy_list.extend(cursor.fetchall())

        return vacancy_list

    def get_vacancies_with_keyword(self, keyword):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова"""

        connection = self.connection
        vacancy_list = []

        with connection.cursor() as cursor:
            cursor.execute('''
                SELECT name, description, area, salary_from, salary_to, salary,
                       currency, experience, employment, address, url
                FROM vacancies
                WHERE LOWER(name) LIKE %s
            ''', (f'%{keyword.lower().strip()}%',))

            vacancy_list.extend(cursor.fetchall())

        return vacancy_list
