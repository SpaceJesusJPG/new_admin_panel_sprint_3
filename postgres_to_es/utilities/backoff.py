import time
from functools import wraps


def backoff(logger, start_sleep_time=1, factor=3, border_sleep_time=30):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * (factor ^ n), если t < border_sleep_time
        t = border_sleep_time, иначе
    :param logger: объект логгинга
    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Ошибка при выполнении {func.__name__}: {e}. Пытаемся восстановить через {sleep_time:.2f} секунд...")
                    time.sleep(sleep_time)
                    sleep_time = min(sleep_time * factor, border_sleep_time)

        return inner

    return func_wrapper
