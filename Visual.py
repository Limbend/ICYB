import matplotlib.pyplot as plt
import seaborn as sns
import dataframe_image as dfi


def regular_events_plot(regular_events, image_full_name):
    plt.rcParams['figure.figsize']=(50,10)
    plot = sns.lineplot(data=regular_events, x='date', y='balance').get_figure()
    plot.savefig(image_full_name)

def df_to_image(dataframe, image_full_name):
    dfi.export(dataframe, image_full_name)