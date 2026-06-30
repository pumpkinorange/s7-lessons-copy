from datetime import datetime, timedelta


def input_paths(date, depth):
    dt = datetime.strptime(date, '%Y-%m-%d')
    return [
        f"/user/s24268544/data/events/"
        f"date={(dt - timedelta(days=offset)).strftime('%Y-%m-%d')}/"
        f"event_type=message"
        for offset in range(depth)
    ]
