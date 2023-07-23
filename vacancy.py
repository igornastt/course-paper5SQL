class Vacancy:
    """
    Класс вакансий работадателей с сайта
    https://hh.ru/
    """

    def __init__(
            self, name: str, description: str, area: str,
            salary_from: int, salary_to: int, currency: str,
            experience: str, employment: str, address: str,
            url: str
    ) -> None:
        self.name = name
        self.description = description
        self.area = area
        self.salary_from = salary_from
        self.salary_to = salary_to
        self.currency = currency
        self.experience = experience
        self.employment = employment
        self.address = address
        self.url = url

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.name}', '{self.description}', " \
               f"'{self.area}', {self.salary_from}, " \
               f"{self.salary_to}, '{self.currency}', '{self.experience}', " \
               f"'{self.employment}', '{self.address}', '{self.url}')"

    def __str__(self) -> str:
        return f'Вакансия {self.name}'


    def salary(self) -> int or None:
        """
        Возвращает среднюю зарплату вакансии
        """
        if (self.salary_from + self.salary_to) == 0:
            return None

        if self.salary_from == 0:
            return self.salary_to

        if self.salary_to == 0:
            return self.salary_from

        salary = int((self.salary_from + self.salary_to) / 2)
        return salary


    def salary_from(self) -> int:
        """
        Возвращает минимальную зарплату вакансии
        """
        return self.salary_from


    def salary_to(self) -> int:
        """
        Возвращает максимальную зарплату вакансии
        """
        return self.salary_to


    def get_vacancies_inf(vacancies_list) -> list[list]:
        """
        Возвращает список, в котором содержатся
        списки.
        """
        vacancies_inf_list = []

        for vacancy in vacancies_list:
            # заменяет значение аттрибутов, содержащих в себе пустую строку, на None
            for attribute_name, attribute_value in vars(vacancy).items():
                if attribute_value == '':
                    setattr(vacancy, attribute_name, None)

            vacancies_inf_list.append(
                [
                    vacancy.name,
                    vacancy.description,
                    vacancy.area,
                    vacancy.salary_from,
                    vacancy.salary_to,
                    vacancy.salary,
                    vacancy.currency,
                    vacancy.experience,
                    vacancy.employment,
                    vacancy.address,
                    vacancy.url
                ]
            )

        return vacancies_inf_list
