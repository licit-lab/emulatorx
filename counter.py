import csv
import datetime


def count_distinct_obs(start, end):
    csv.register_dialect('custom', 'excel', delimiter=';')
    formatter = "%d/%m/%Y %H:%M:%S"
    start_time = datetime.datetime.strptime(start, formatter)
    end_time = datetime.datetime.strptime(end, formatter)
    distinct_obs = set([])
    distinct_links = set([])
    with open('final_all_observations.csv', newline='') as f:
        reader = csv.DictReader(f, dialect='custom')
        for row in reader:
            obs_time = datetime.datetime.strptime(row['timestamp'], formatter)
            if start_time <= obs_time <= end_time:
                distinct_obs.add(row['id'])
                distinct_links.add(row['linkid'])
        print("Number of distinct observations: " + len(distinct_obs))
        print("Number of distinct links: " + len(distinct_links))


if __name__ == '__main__':
    count_distinct_obs('06/09/2018 00:00:00', '06/09/2018 02:00:00')
