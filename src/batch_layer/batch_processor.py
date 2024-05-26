from pyspark.sql.functions import col, countDistinct, sum, count, when, lit


def process_batch(spark, file_path):
    batsman_file_location = './batch_processed_data/batsman_metrics'
    bowler_file_location = './batch_processed_data/bowler_metrics'
    match_file_location = './batch_processed_data/match_metrics'

    ipl_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
    ipl_df = ipl_df.na.fill(value=0, subset=['wides', 'byes', 'legbyes', 'runs_off_bat','noballs'])
    
    batsman_metrics_df = process_batsman_metrics(ipl_df)
    write_to_parquet(batsman_metrics_df, batsman_file_location)
    
    bowler_metrics_df = process_bowler_metrics(ipl_df)
    write_to_parquet(bowler_metrics_df, bowler_file_location)

    match_metrics_df = process_match_metrics(ipl_df)
    write_to_parquet(match_metrics_df, match_file_location)


def process_batsman_metrics(ipl_df):
    print('batsman metrics')
    batsman_metrics_df = ipl_df.groupBy('striker', 'season', 'batting_team') \
                         .agg(countDistinct('match_id').alias('matches_played'),
                              sum('runs_off_bat').alias('runs_scored'),countDistinct('match_id','ball').alias('balls_played')
    ).orderBy('runs_scored','matches_played',ascending=False)
    return batsman_metrics_df


def process_bowler_metrics(ipl_df):
    print('bowler metrics')
    bowler_metrics_df = ipl_df.groupBy('bowler', 'season', 'bowling_team') \
                            .agg(countDistinct('match_id').alias('matches_played'),
                                sum('wides').alias('total_wides_bowled'),
                                sum('noballs').alias('total_no_balls_bowled'),
                                sum('legbyes').alias('total_leg_byes'),
                                sum('runs_off_bat').alias('runs_given'),
                                countDistinct('match_id','ball').alias('balls_bowled'),
                                count(when(col('wicket_type').isNotNull(), 1)).alias('total_no_of_wickets')
                                )
    return bowler_metrics_df


def process_match_metrics(ipl_df):
    print('match metrics')
    match_innings_metrics_df = ipl_df.groupBy('match_id','innings','batting_team','bowling_team').agg(
                              sum(col('runs_off_bat')+col('wides')+col('noballs')+col('byes')+col('legbyes')).alias('total_runs'),
                              count(when(col('wicket_type').isNotNull(), 1)).alias('no_of_wickets')
                              )
    match_innings_metrics_df_alias1 = match_innings_metrics_df.alias("df1")
    match_innings_metrics_df_alias2 = match_innings_metrics_df.alias("df2")

    joined_df = match_innings_metrics_df_alias1.join(
        match_innings_metrics_df_alias2,
        (col("df1.match_id") == col("df2.match_id")) & (col("df1.innings") != col("df2.innings")),
        how="inner"
    )

    match_metrics_df = joined_df.filter((col("df1.innings") == 1) & (col("df2.innings") == 2)).select(
        col("df1.match_id").alias("match_id"),
        when(col("df1.total_runs") > col("df2.total_runs"), col("df1.batting_team")).otherwise(col("df2.batting_team")).alias("winning_team"),
        when(col("df1.total_runs") > col("df2.total_runs"), col("df2.batting_team")).otherwise(col("df1.batting_team")).alias("losing_team"),
        when(col("df1.total_runs") > col("df2.total_runs"), col("df1.total_runs")).otherwise(col("df2.total_runs")).alias("winning_team_score"),
        when(col("df1.total_runs") > col("df2.total_runs"), col("df1.no_of_wickets")).otherwise(col("df2.no_of_wickets")).alias("winning_team_wickets"),
        when(col("df1.total_runs") > col("df2.total_runs"), col("df2.total_runs")).otherwise(col("df1.total_runs")).alias("losing_team_score"),
        when(col("df1.total_runs") > col("df2.total_runs"), col("df2.no_of_wickets")).otherwise(col("df1.no_of_wickets")).alias("losing_team_wickets"),
        when(col("df1.total_runs") > col("df2.total_runs"), lit("Batted First")).otherwise(lit("Chased")).alias("winning_team_batting_status")
    )
    return match_metrics_df


def write_to_parquet(df, file_location):
    df.write.mode("overwrite").parquet(file_location)

