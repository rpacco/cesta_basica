import pandas as pd


# Calculate variation percentage using dictionary
def calculate_variation(row, mean_dict):
    last_week = row['week'] - 1  # We want the mean from the previous week
    if last_week in mean_dict:
        last_week_mean = mean_dict[last_week]
        variation = ((row['adjusted_price'] - last_week_mean) / last_week_mean) * 100
        return variation
    else:
        return 0  # If there's no previous week, return 0

def cb_daily_text(df: pd.DataFrame):
    cb_prices = df.groupby(['date', 'category']).agg(
        {'adjusted_price': 'mean'}
    ).reset_index().groupby('date').sum()['adjusted_price'].reset_index()
    cb_prices['week'] = cb_prices['date'].dt.isocalendar().week
    # Group by week and calculate mean of adjusted_price
    weekly_means = cb_prices.groupby('week')['adjusted_price'].mean().reset_index()
    weekly_means.columns = ['week', 'mean_price_last_week']
    weekly_means.set_index('week', inplace=True)
    weekly_means_dict = weekly_means['mean_price_last_week'].to_dict()
    cb_prices['daily_variation_percent'] = cb_prices.apply(lambda row: calculate_variation(row, weekly_means_dict), axis=1)
    today_date = cb_prices['date'].iloc[-1].date().strftime('%d/%m/%Y')
    variation_pct = cb_prices['daily_variation_percent'].iloc[-1]
    tweet_text = (
        f'🧺💵 Variação do preço da cesta básica no dia de hoje, {today_date}, em relação à média da semana passada:\n
        {variation_pct:.2f}%'
    )