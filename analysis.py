import os
import contextily as ctx
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colorbar import ColorbarBase
from matplotlib.colors import BoundaryNorm, LinearSegmentedColormap

from retrievers import get_gfas, get_regulation_counts, get_regulation_plot_counts,\
    get_frequent_regulation_instances, get_plot_ids


def get_regulation_overview_df(endpoint, reg_names, plots, out_dir=None):
    """
    Retrieves an overview of regulation counts for a list of regulation types and optionally writes to a CSV file.

    :param endpoint: The SPARQL endpoint URL to query.
    :param reg_names: A list of regulation type names to count (e.g., ["HeightControlPlan", "ConservationArea"]).
    :param plots: GeoDataFrame containing plot information with 'plots' and 'area' columns.
    :param out_dir: Optional; Directory path to save the output CSV file. If None, no file is written.
    :return: A DataFrame with regulation names as the index and their counts, linked plots, and total plot area.
    """

    counts = {}
    plot_counts = {}
    plot_areas = {}

    for reg_name in reg_names:
        counts['Masterplan'] = len(plots)
        counts[reg_name] = get_regulation_counts(endpoint, reg_name)

        reg_plots = get_regulation_plot_counts(endpoint, reg_name)
        linked_plots = plots[plots['plots'].isin(list(set(reg_plots['plots'])))]
        plot_counts['Masterplan'] = len(plots)
        plot_counts[reg_name] = len(linked_plots)

        plot_areas['Masterplan'] = round(plots['plot_area'].sum() / 1e6, 3)
        plot_areas[reg_name] = round(linked_plots['plot_area'].sum() / 1e6, 3)

    df = pd.DataFrame({
        'counts': pd.Series(counts),
        'linked_plots': pd.Series(plot_counts),
        'linked_area': pd.Series(plot_areas),
    })

    if out_dir:
        output_file = os.path.join(out_dir, 'output_table_2.csv')
        df.to_csv(output_file, index=True)

    return df


def get_regulation_instance_overview_df(endpoint, plots, out_dir=None):
    """
    Process regulations to sample duplicates, fetch plots they apply to, and calculate the plot area.

    :param endpoint: SPARQL endpoint to query plot details.
    :param plots: GeoDataFrame containing plot information with 'plots' and 'plot_area' columns.
    :param out_dir: Optional; Directory path to save the output CSV file. If None, no file is written.
    :return: A DataFrame summarizing regulations and their respective plot areas.
    """

    sampled_regs = get_frequent_regulation_instances(endpoint)
    results = []

    for _, row in sampled_regs.iterrows():
        reg_id = row['reg']

        plot_ids = get_plot_ids(endpoint, reg_id)
        subset_plots = plots[plots['plots'].isin(plot_ids)]
        total_area = round(subset_plots['plot_area'].sum() / 1e6, 3)

        results.append({
            'type': row['type'],
            'programmes': row['programmes'],
            'zones': row['zones'],
            'plots_count': len(plot_ids),
            'total_area': total_area
        })

    df = pd.DataFrame(results)[:10]

    if out_dir:
        output_file = os.path.join(out_dir, 'output_table_3.csv')
        df.to_csv(output_file, index=True)
        print(f"DataFrame written to {out_dir}")

    return df


def get_gfa_overview(endpoint, plots, non_gfa_plots, out_dir):
    """
    Generates an overview of GFA distribution across zoning types and saves it as a CSV.

    :param out_dir: directory to save the output.
    :param endpoint: URL of the SPARQL endpoint to query for GFA data.
    :param plots: GeoDataFrame containing plot data with 'plots', 'zone', 'gpr', and 'plot_area' columns.
    :param non_gfa_plots: List of plot identifiers to exclude from the analysis.
    """
    # Calculate the metrics
    plot_counts = plots.groupby('zone')['plots'].size().sort_index()
    gpr_counts = plots.groupby('zone')['gpr'].apply(lambda x: x.notna().sum()).sort_index()
    gpr_percent = round((gpr_counts / plot_counts) * 100, 2).rename('gpr_%')

    gfas = get_gfas(endpoint)
    gfas_counts = gfas.groupby("plots")["gfa_value"].size().rename('gfa_counts')
    plots = plots.merge(gfas_counts, on='plots', how='outer')

    gfas = gfas.groupby("plots")["gfa_value"].min()
    plots = plots.merge(gfas, on='plots', how='outer')
    gfa_counts = plots.groupby('zone')['gfa_value'].apply(lambda x: x.notna().sum()).sort_index().rename('gfa')
    gfa_percent = round((gfa_counts / plot_counts) * 100, 2).rename('gfa_%')

    excl_plots = plots.loc[plots['plots'].isin(non_gfa_plots)].groupby('zone')['plots'].count().astype(int).fillna(
        0).sort_index().rename('excl.')
    multiple_gfas = plots.loc[plots['gfa_counts'] > 1].groupby('zone')['plots'].count().sort_index().rename('> 1 GFAs')
    gfas_per_plot = round(plots.groupby('zone')['gfa_counts'].mean().sort_index(), 2).rename('GFAs / plot')

    df = pd.concat(
        [plot_counts, gpr_counts, gpr_percent, gfa_counts, gfa_percent, excl_plots, multiple_gfas, gfas_per_plot],
        axis=1, join='outer'
    )

    # Prepare the summary row, ignoring zeros in mean calculations
    summary_row = {
        'plots': plot_counts.sum(),
        'gpr': gpr_counts.sum(),
        'gpr_%': round((gpr_counts.sum() / plot_counts.sum() * 100), 2),
        'gfa': gfa_counts.sum(),
        'gfa_%': round((gfa_counts.sum() / plot_counts.sum() * 100), 2),
        'excl.': excl_plots.sum(),
        '> 1 GFAs': multiple_gfas.sum(),
        'GFAs / plot': gfas_per_plot[gfas_per_plot != 0].mean()
    }

    df.loc['Summary'] = summary_row

    if out_dir:
        output_file = os.path.join(out_dir, 'output_table_4.csv')
        df.to_csv(output_file, index=True)
        print(f"DataFrame written to {out_dir}")

    return df


def plot_iop(plots, reg_links, out_dir):
    """
    Generates and saves a plot showing the number of linked regulations for each plot.

    :param reg_links: DataFrame with regulation and plot links.
    :param plots: GeoDataFrame containing plots data.
    :param out_dir: Directory to save the output image.
    """

    reg_count = reg_links.groupby('plots').size().reset_index(name='iop')
    plots = plots.merge(reg_count, on='plots')

    # Define boundaries and colormap
    boundaries = np.arange(0, max(reg_links.groupby('plots')['reg'].nunique()), 1)
    custom_cmap = LinearSegmentedColormap.from_list('CustomGradient', ["#aebfb6", "#902925"])
    norm = BoundaryNorm(boundaries, custom_cmap.N)

    # Create the figure and axes
    fig, ax = plt.subplots(figsize=(8, 8), dpi=150)
    plots.plot(column='iop', cmap=custom_cmap, norm=norm, ax=ax, alpha=0.9)
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, alpha=0.5)

    ax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    cbar_ax = fig.add_axes([ax.get_position().x0, ax.get_position().y0 - 0.03, ax.get_position().width, 0.02])
    cbar = ColorbarBase(cbar_ax, cmap=custom_cmap, norm=norm, boundaries=boundaries, orientation='horizontal')
    cbar.set_label('Number of linked regulations to the plot')

    # Save the figure
    output_path = os.path.join(out_dir, "output_figure_5.png")
    plt.savefig(output_path, bbox_inches='tight', dpi=150)
    plt.close(fig)

    print(f"Figure saved to {output_path}")


def plot_gfa_deltas(endpoint, plots, non_gfa_plots, out_dir):
    """
    Generates and saves a plot showing the relative difference (delta) between
    the GFA (Gross Floor Area) retrieved from the knowledge graph (KG)
    and the GFA computed via GPR (gpr_gfa).

    :param non_gfa_plots: plot IDs excluded from GFA calculation.
    :param endpoint: The URL of the SPARQL endpoint to query for GFA data.
    :param plots: A GeoDataFrame containing plot geometries and attributes including 'plots', 'gpr', and 'plot_area'.
    :param out_dir: The directory path where the output plot image (PNG) will be saved.
    """

    gfa_plots = plots[~plots['plots'].isin(non_gfa_plots)].copy()

    gfa_plots['gpr_gfa'] = gfa_plots['gpr'] * gfa_plots['plot_area']
    gfas = get_gfas(endpoint)
    gfas = gfas.groupby("plots")["gfa_value"].min()
    gfa_plots = gfa_plots.merge(gfas, on='plots', how='inner')

    gfa_plots['gpr_gfa_delta'] = ((gfa_plots['gfa_value'] - gfa_plots['gpr_gfa']) / gfa_plots['gpr_gfa']) * 100

    delta_min = gfa_plots['gpr_gfa_delta'].min()
    delta_max = gfa_plots['gpr_gfa_delta'].max()
    norm = plt.Normalize(vmin=delta_min, vmax=delta_max)

    custom_cmap = LinearSegmentedColormap.from_list('CustomGradient', ["#902925", "#aebfb6"])
    fig, ax = plt.subplots(figsize=(8, 8), dpi=150)
    gfa_plots.plot(column='gpr_gfa_delta', cmap=custom_cmap, norm=norm, ax=ax, alpha=0.9)
    ax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, alpha=0.5)
    cbar_ax = fig.add_axes([ax.get_position().x0, ax.get_position().y0 - 0.03, ax.get_position().width, 0.02])

    sm = plt.cm.ScalarMappable(norm=norm, cmap=custom_cmap)
    sm.set_array([])
    cbar = plt.colorbar(sm, cax=cbar_ax, orientation='horizontal')
    cbar.set_label("Relative GFA Δ")

    # Save the figure
    output_path = os.path.join(out_dir, "output_figure_6.png")
    plt.savefig(output_path, bbox_inches='tight', dpi=150)

    plt.close(fig)

