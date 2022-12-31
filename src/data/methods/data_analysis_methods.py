import io

import numpy as np
from slugify import slugify

# Visualization libraries
import matplotlib.pyplot as plt
import folium
import branca
import branca.colormap as cm
from PIL import Image
import selenium

import geohash as gh

def plot_price_hist(df, col, bins, title, xlabel, figsize, xlabel_rotation=0, barlabel_rotation=-1, show=True, save_path=""):
    df = df.dropna(subset=col)
    
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=figsize)
    
    values, bins, bars = plt.hist(df[col], bins, edgecolor='black', linewidth=1.2)
    
    if barlabel_rotation != -1:
        bar_label_v_spacing = axes.get_ylim()[1]*0.06
        for p in axes.patches:
            h, w, x = p.get_height(), p.get_width(), p.get_x()
            xy = (x + w / 2., h + bar_label_v_spacing)
            if (not np.isnan(h)) and h != 0:
                axes.annotate(text=h, xy=xy, ha='center', va='center', rotation=barlabel_rotation)
    
    axes.set_ylim([axes.get_ylim()[0], axes.get_ylim()[1]*1.1])
    
    plt.xlabel(xlabel)
    plt.ylabel("No. of listings")
    plt.title(title)

    plt.xticks(rotation=xlabel_rotation)
   
    fig.tight_layout()
    
    if save_path != "":
        plt.savefig(save_path)
    
    if show:
        plt.show()    

def plot_number_of(df, col, title, xlabel, figsize, xticks_rotation=0, show=True, save_path=""):
    df = df.dropna(subset=col)
    labels, counts = np.unique(df[col], return_counts=True)
    labels = np.array(labels).astype('str').tolist()
       
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=figsize)
    
    bars = plt.bar(labels, counts, align='center')
    
    plt.bar_label(bars)
    
    plt.xlabel(xlabel)
    plt.ylabel("Count")
    plt.title(title)
    
    axes.set_xticks(labels, labels, rotation=xticks_rotation, ha=('right' if xticks_rotation==45 else 'center'))
   
    fig.tight_layout()
    
    if save_path != "":
        plt.savefig(save_path)
    
    if show:
        plt.show()   

def plot_encoded_columns(df, encoded_columns, show=True, save_path=""):
       
    for col in encoded_columns:
        if save_path != "":
            x = slugify(col)
            path_filename = save_path + f"apartments_airbnb_with_{x}.png"
        
        plot_number_of(df = df, 
                    col = col, 
                    title = f'Apartments on Airbnb with\n\'{col}\'', 
                    xlabel = '',
                    figsize = (6,6),
                    show=show,
                    save_path=path_filename)

def bar_plot(df, x, y, title, xlabel, ylabel, figsize, xticks_rotation=0, barlabel_rotation=0, show=True, save_path=""):
             
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=figsize)
    
    bars = plt.bar(df[x], df[y], align='center')
    
    plt.bar_label(bars, rotation=barlabel_rotation)
    
    if barlabel_rotation != 0:
        axes.set_ylim([axes.get_ylim()[0], axes.get_ylim()[1]*1.1])
    
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    
    axes.set_xticks(df[x], df[x], rotation=xticks_rotation, ha=('right' if xticks_rotation==45 else 'center'))
    
    plt.xticks(rotation=xticks_rotation)
   
    fig.tight_layout()
    
    if save_path != "":
        plt.savefig(save_path)
    
    if show:
        plt.show()   

def get_top3_locations(df, sort_by, ascending=False):
    
    top3 = df.sort_values('Mean price', ascending=ascending)['geohash'].head(3).values
    
    return top3

def add_geohash_to_map(m, geohash, popup, color='green'):
    decoded = gh.bbox(geohash)

    W = decoded['w']
    E = decoded['e']
    N = decoded['n']
    S = decoded['s']

    # create each point of the rectangle
    upper_left = (N, W)
    upper_right = (N, E)
    lower_right = (S, E)
    lower_left = (S, W)

    edges = [upper_left, upper_right, lower_right, lower_left]

    # create rectangle object and add it to the map canvas
    folium.Rectangle(
        bounds=edges,
        fill=True,
        color=color,
        weight=1,
        popup=popup,
        fill_opacity=0.6
    ).add_to(m)

def df_to_geohash_on_map(df, m, color_based_on,  legend, colors=['gray','red']):

    # Define colormap for this specific dataframe
    vmin = df[color_based_on].min()
    vmax = df[color_based_on].max()
    colormap = cm.LinearColormap(colors=colors, index=[vmin, vmax],vmin=vmin,vmax=vmax)
    colormap.caption = legend

    def add_to_map(x):
        location = x['geohash']
        color = colormap(x[color_based_on])
        popup = f"Geohash: {x['geohash']}\n\n\
                Listings count: {x['Listings count']}\n\n\
                Mean price: {x['Mean price']:.0f}\n\n\
                Reviews: {x['Sum of reviews']}\n\n\
                Mean availability: {x['Mean availability']}"
        add_geohash_to_map(m, location, popup, color)
    
    df.apply(add_to_map, axis=1)
    
    m.add_child(colormap)

def create_map(center, zoom_start):
    lat, long = center

    # create a map canvas
    m = folium.Map(
        location=[lat,long], # set the center location of the map
        zoom_start=zoom_start,
        tiles="CartoDB Positron"
    )
    
    return m

def create_map_analysis(df, center, zoom_start, color_based_on, legend, save_path=""):

    m = create_map(center, zoom_start)
    
    df_to_geohash_on_map(df, m, color_based_on, legend)
    
    if save_path != "":
        img_data = m._to_png(5)
        img = Image.open(io.BytesIO(img_data))
        img.save(save_path)