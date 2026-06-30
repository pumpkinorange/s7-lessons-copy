/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster \
  verified_tags_candidates.py 2022-05-31 7 100 \
  /user/prod/data/events \
  /user/prod/data/snapshots/tags_verified/actual \
  /user/prod/data/analytics/verified_tags_candidates_d7
