import pendulum
from datetime import timedelta
from typing import Optional

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


class TimeInterval_ragged(Timetable):
    # метод ручного запуска
    def infer_manual_data_interval(self, run_after: pendulum.DateTime) -> DataInterval:
        
        ##ручной запуск сразу после триггера
        start = run_after
        end = run_after

        return DataInterval(start=start, end=end)

    def next_dagrun_info(self, *, last_automated_data_interval: Optional[DataInterval], restriction: TimeRestriction,) -> Optional[DagRunInfo]:
        UTC = pendulum.timezone('UTC')
        # задаем интервал при ранее успешных автозапусках
        if last_automated_data_interval is not None:

            day = timedelta(days=1)
            five_min = timedelta(minutes=5) 
            twenty_min = timedelta(minutes=20) 

            last_start = last_automated_data_interval.start

            ## Условие выполнения: время последнего триггера больше 7:00 (3:xx UTC) и меньше 9:xx (4:00 UTC)
            if (last_start.hour > 0 )  and (last_start.hour < 3): 
                next_start = (last_start+twenty_min).replace(tzinfo=UTC)
                next_end = (last_start+twenty_min).replace(tzinfo=UTC)
            ## Условие выполнения: время последнего триггера больше 9:00 (3:xx UTC) и меньше 10:xx (4:00 UTC)
            elif last_start.hour == 3: 
                next_start = (last_start+five_min).replace(tzinfo=UTC)
                next_end = (last_start+five_min).replace(tzinfo=UTC)
            ## Условие выполнения: время последнего триггера больше 22:xx (16:xx UTC) и меньше 23:xx (17:xx UTC)
            elif last_start.hour == 16: 
                next_start = (last_start+twenty_min).replace(tzinfo=UTC)
                next_end = (last_start+twenty_min).replace(tzinfo=UTC)
            ## Условие выполнения: else переносим на след.день в 3:00 UTC
            else: 
                next_start = (last_start+day).set(hour=1, minute=0).replace(tzinfo=UTC)
                next_end = (last_start+day).set(hour=1, minute=0).replace(tzinfo=UTC)
        # задаем интервал при отсутствии ранее успешных автозапусках
        else:
            next_start = restriction.earliest
        ## если нет данных о star_date - не запускаем автоматичеки
            if next_start is None:
                return None
        ## если catchup=False, то запуск сегодня
            if not restriction.catchup: 
                next_start = max(next_start, pendulum.DateTime.combine(pendulum.Date.today(), pendulum.Time.min).replace(tzinfo=UTC))
            next_start = next_start.set(hour=17, minute=0).replace(tzinfo=UTC)
            next_end = next_start.set(hour=17, minute=0).replace(tzinfo=UTC)
        ## Если мы вышли за дату последнего запуска
            if restriction.latest is not None and next_start > restriction.latest:
                return None
        return DagRunInfo.interval(start=next_start, end=next_end)
         
    
class Plugin_TimeInterval_ragged(AirflowPlugin):
    name = "schedule_interval_ragged"
    timetables = [TimeInterval_ragged]
