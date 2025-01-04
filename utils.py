import os.path
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import configparser
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Polygon, MultiPolygon, LineString
from shapely.ops import unary_union
import logging

logger = logging.getLogger(__name__)


def get_query_result(endpoint, query):
    """
    Executes a SPARQL query against a specified endpoint and returns the results as a pandas DataFrame.

    :param endpoint: The SPARQL endpoint URL to which the query will be sent.
    :param query: The SPARQL query string to be executed.
    :return: A pandas DataFrame containing the query results with 'value' fields extracted.
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)

    sparql.setReturnFormat(JSON)
    qr = sparql.query().convert()

    qr = pd.DataFrame(qr['results']['bindings'])
    result_df = qr.applymap(lambda cell: cell['value'], na_action='ignore')
    result_df.reset_index()

    return result_df


def string_to_polygon(geom_string, geodetic=False, flip=True):
    """
    The function process queried geometries in the KG datatype and transforms it into WKT.
    :param geom_string: geometry string as stored in the KG.
    :param geodetic: boolean indicating whether the coordinate reference system is in degrees or metres.
    :param flip: boolean indicating whether original x and y coordinates are in wrong order.
    :return: geometry as WKT.
    """

    points_str = geom_string.split('#')
    num_of_points = int(len(points_str) / 3)
    points = []

    for i in range(num_of_points):
        start_index = i * 3
        x, y, z = float(points_str[start_index]), float(points_str[start_index + 1]), float(points_str[start_index + 2])
        if geodetic:
            if flip:
                points.append((y, x))
            else:
                points.append((x, y))
        else:
            points.append((x, y, z))

    return Polygon(points)


def load_config(file_path: str):
    """
    Load and parse the configuration file.

    :param file_path: Path to the configuration file.
    :return: ConfigParser object.
    """

    config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
    config.read(file_path)
    root_directory = os.path.dirname(os.path.abspath(__file__))
    config.set('paths', 'root', root_directory)

    return config


def process_plots(plots):
    """
    Filters and cleans a GeoDataFrame containing plot geometries.

    :param plots: A GeoDataFrame containing plot geometries and associated data.
    :return: A cleaned GeoDataFrame with simplified geometries, invalid geometries removed, and min area filter applied.
    """

    plots.geometry = plots.geometry.simplify(0.1)
    plots.loc[:, 'geometry'] = plots.loc[:, 'geometry'].buffer(0)
    plots = plots[~(plots.geometry.type == "MultiPolygon")]
    plots = plots.loc[plots.area >= 50]

    return plots


def get_neighbor_links(plots):
    """
    Generates neighbor links between plots based on spatial intersections.

    :param plots: GeoDataFrame containing plot geometries and their identifiers.
    :return: GeoDataFrame containing links between neighboring plots.
    """
    all_plots = plots[['plots', 'geometry']].rename(columns={'plots': 'context_plots'})
    plots['geometry'] = plots.buffer(2, cap_style=3)

    intersection = gpd.overlay(plots, all_plots, how='intersection', keep_geom_type=True)
    intersection['area'] = intersection.area
    neighbor_links = intersection.loc[lambda df: df['area'] > 1]

    logger.info(f"{len(neighbor_links)} neighbor relationships derived")

    return neighbor_links


def get_residential_area(res_plots, buffer, hole_size):
    """
    Generate an abstract residential area boundary by merging buffered residential plot polygons.

    :param res_plots: A GeoDataFrame containing residential plot geometries.
    :param buffer: Buffer size to expand the plot boundaries.
    :param hole_size: Minimum area of holes to retain in the polygons.
    :return: A GeoDataFrame representing the simplified residential area boundaries.
    """

    res_area = res_plots.buffer(buffer).unary_union
    multipolygon_parts = []
    for polygon in res_area.geoms:
        list_interiors = []
        for interior in polygon.interiors:
            if Polygon(interior).area > hole_size:
                list_interiors.append(interior)
        multipolygon_parts.append(Polygon(polygon.exterior.coords, holes=list_interiors))
    res_area = MultiPolygon(multipolygon_parts).simplify(0.5).buffer(-abs(buffer))
    res_area_df = gpd.GeoDataFrame(geometry=gpd.GeoSeries(res_area), crs=3857)
    res_area_df = res_area_df.explode('geometry', ignore_index=True)

    return res_area_df


def check_fringe(res_plots, residential_area, intersection_buffer):
    """
    Check if residential plots are on the fringe of a residential area and mark them with a 'fringe' column.

    :param res_plots: A GeoDataFrame of residential plot geometries.
    :param residential_area: A GeoDataFrame representing the residential area boundary.
    :param intersection_buffer: Buffer size to expand the residential plots for intersection checks.
    :return: A GeoDataFrame of residential plots with an added 'fringe' column indicating fringe status.
    """

    res_plots_buffered = res_plots.copy()
    res_plots_buffered['geometry'] = res_plots_buffered.buffer(intersection_buffer)
    res_plots_buffered['area'] = res_plots_buffered.area
    intersection = gpd.overlay(residential_area, res_plots_buffered, how='intersection')
    intersection['intersection_area'] = intersection.area
    intersection_fringe = intersection[round(intersection['intersection_area']) < round(intersection['area'])]
    res_plots['fringe'] = res_plots['plots'].isin(intersection_fringe['plots'])

    return res_plots


def find_neighbours(plots, all_plots):
    """
    Find neighboring residential plots and add a 'neighbor_list' column with neighbor IDs.

    :param plots: A GeoDataFrame containing residential plot geometries.
    :param all_plots: A GeoDataFrame containing all plots for neighbor identification.
    :return: A GeoDataFrame with an added 'neighbor_list' column indicating neighbor plot IDs.
    """

    buffered_plots = plots[['plots', 'geometry']].copy()
    buffered_plots['geometry'] = buffered_plots.buffer(2, cap_style=3)
    intersection = gpd.overlay(buffered_plots, all_plots, how='intersection', keep_geom_type=True)
    intersection['area'] = intersection.area
    intersection = intersection.loc[intersection['area'] > 1, :].drop(columns={'area', 'geometry'})
    neighbors = intersection.groupby('plots')['context_plots'].unique()
    plots['neighbor_list'] = neighbors.loc[plots['plots']].to_numpy()

    return plots


def get_edges(polygon):
    """
    Extract the edges of a geometry.

    :param polygon: A shapely Polygon object.
    :return: A list of LineString objects representing the edges of the polygon.
    """

    curve = polygon.exterior.simplify(0.1)
    return list(map(LineString, zip(curve.coords[:-1], curve.coords[1:])))


def get_min_rect_edge_df(res_plots):
    """
    Create an edge DataFrame with minimum rectangle edges exploded and buffered.

    :param res_plots: A GeoDataFrame containing residential plots with a 'min_rect_edges' column.
    :return: A GeoDataFrame with exploded and buffered minimum rectangle edges.
    """

    edges = res_plots.loc[:, ['plots', 'min_rect_edges']].copy().sort_values('plots').explode('min_rect_edges',
                                                                                              ignore_index=True)
    edges["order"] = list(range(4)) * res_plots['plots'].nunique()
    edges = gpd.GeoDataFrame(edges, geometry='min_rect_edges', crs=3857)
    edges['length'] = edges.length.round(decimals=3)
    edges = edges.sort_values("plots")
    edges['min_rect_edges'] = edges.buffer(3, single_sided=False, cap_style=3)
    edges['buffered_area'] = edges.area

    return edges


def intersect_roads_with_edges(edges, plots, res_plots):
    """
    Intersect buffered edge DataFrame with road plots and filter out intersections with non-neighbor roads.

    :param edges: A GeoDataFrame containing buffered edges of residential plots.
    :param plots: A GeoDataFrame containing all plots, including road zones.
    :param res_plots: A GeoDataFrame containing residential plots with neighbor information.
    :return: A GeoDataFrame of intersections between edges and roads that are neighbors of residential plots.
    """

    intersection = gpd.overlay(edges, plots[plots['zone'] == 'ROAD'], how='intersection')
    neighbor = (intersection.merge(res_plots.loc[:, ['plots', 'neighbor_list']], how="left", left_on="plots_1",
                                   right_on='plots')
                .drop(columns=["plots"])
                .apply(lambda row: row["plots_2"] in row["neighbor_list"], axis=1))
    intersection = intersection.loc[neighbor, :]
    intersection['intersection_area'] = intersection.area

    return intersection


def set_min_rect_edge_types(intersection, edges, plots):
    """
    Assign minimum rectangle edge types (front, rear, and side edges) to residential plots.

    :param intersection: A GeoDataFrame containing intersections of plot edges with roads or other features.
    :param edges: A GeoDataFrame containing edges of residential plots with their lengths and order indices.
    :param plots: A DataFrame containing plot details to which edge types will be added.
    :return: A DataFrame with updated plot details including front, rear, and side edge indices.
    """

    # define a front edge indice based on largest intersection with the longest edge.
    front_edge = (intersection
                  .sort_values(by=['plots_1', 'intersection_area', 'length'], ascending=False)
                  .groupby(['plots_1'])['order']
                  .first()
                  )
    front_edge.name = 'min_rect_front_edge'
    plots = plots.merge(front_edge, left_on='plots', right_index=True, how='left')

    not_front_edge = edges["order"] != plots.sort_values('plots')['min_rect_front_edge'].repeat(4).to_numpy()
    front_edge_length = edges.loc[~not_front_edge, ["plots", 'length']].set_index("plots")['length']
    front_edge_length_missing = list(set(plots["plots"].unique()).difference(front_edge_length.index))
    front_edge_length = pd.concat(
        [front_edge_length, pd.Series([0] * len(front_edge_length_missing), index=front_edge_length_missing)])
    front_edge_length = front_edge_length.sort_index().repeat(4)

    # define a rear edge indice which is not a front edge indice but the same length.
    rear_edge = not_front_edge & (edges["length"] == front_edge_length.to_numpy())
    rear_edge = edges.loc[rear_edge, :].groupby("plots")["order"].first()
    rear_edge.name = 'min_rect_rear_edge'
    plots = plots.merge(rear_edge, left_on='plots', right_index=True, how='left')

    # define side edge indices which are the remaining indices that are not front or rear edge indices.
    edge_indices = [list({0.0, 1.0, 2.0, 3.0}.difference([plots.loc[x, 'min_rect_front_edge'],
                                                          plots.loc[x, 'min_rect_rear_edge']])) for x in plots.index]
    plots['min_rect_side_edges'] = [edge_indices[i] for i in range(len(edge_indices))]
    plots.loc[plots['min_rect_front_edge'].isna(), 'min_rect_side_edges'] = np.nan

    return plots


def is_corner_plot(intersection, min_rect_plots, overlap_ratio):
    """
    Determine if a plot is a corner plot by checking if its minimum rectangle's
    front and side edges intersect with roads at least a specified overlap ratio.

    :param intersection: A DataFrame containing intersections of plot edges with roads or other features.
    :param min_rect_plots: A DataFrame containing plots and their minimum rectangle edge details.
    :param overlap_ratio: A float representing the minimum overlap ratio to qualify as a corner plot.
    :return: A DataFrame with an additional column 'is_corner_plot' indicating whether a plot is a corner plot.
    """

    road_edges = intersection.loc[(intersection['intersection_area'] / intersection['buffered_area']) > overlap_ratio].groupby('plots_1')['order'].unique()
    road_edges = (pd.merge(road_edges, min_rect_plots.loc[:, ['plots', 'min_rect_rear_edge']]
                           .set_index('plots'), how='left', right_index=True, left_index=True))

    road_edges.loc[:, 'min_rect_rear_edge'] = road_edges['min_rect_rear_edge'].astype(int)
    min_rect_plots = (min_rect_plots
                      .merge(road_edges.apply(is_corner_plot_helper, axis=1)
                             .rename('is_corner_plot'), how='left', left_on='plots', right_index=True))

    return min_rect_plots


def is_corner_plot_helper(road_edge_row):
    return len(set(road_edge_row.loc['order']).difference([road_edge_row.loc['min_rect_rear_edge']])) > 1


def find_average_width_or_depth(plot_row, width):
    """
    Calculate the average width or depth of a plot using the minimum rectangle's front and side edges.

    :param plot_row: A pandas Series representing a plot row with the following keys:
                     'min_rect_edges', 'min_rect_front_edge', 'min_rect_side_edges', and 'geometry'.
    :param width: A string specifying whether to calculate 'average_width' or 'average_depth'.
    :return: The average width or depth as a float.
    """

    cur_front_edge = plot_row.loc['min_rect_edges'][int(plot_row.loc['min_rect_front_edge'])]
    cur_side_edge = plot_row.loc['min_rect_edges'][int(plot_row.loc['min_rect_side_edges'][0])]
    if width == 'average_width':
        offset_distances = np.linspace(0, cur_side_edge.length, 12)[1:-1]
        lines = [cur_front_edge.parallel_offset(cur_offset, 'left') for cur_offset in offset_distances]
    else:
        offset_distances = np.linspace(0, cur_front_edge.length, 12)[1:-1]
        lines = [cur_side_edge.parallel_offset(cur_offset, 'left') for cur_offset in offset_distances]
    average_length = round(np.median([plot_row.loc['geometry'].intersection(line).length for line in lines]), 3)

    return average_length


def set_residential_plot_properties(res_plots, plots):
    """
    Set residential plot properties such as fringe plot status, corner plot status,
    and average width and depth for each plot.

    :param res_plots: A GeoDataFrame containing residential plots with geometry and plot identifiers.
    :param plots: A GeoDataFrame containing all plots with geometry and plot identifiers.
    :return: A GeoDataFrame with updated residential plot properties including 'fringe',
             'neighbor_list', 'corner_plot', 'average_width', and 'average_depth'.
    """

    residential_area = get_residential_area(res_plots, 200, 120000)
    res_plots = check_fringe(res_plots.copy(), residential_area, 10)
    logger.info('fringe plots set.')

    all_plots = plots.loc[:, ['plots', 'geometry']]
    all_plots.rename(columns={'plots': 'context_plots'}, inplace=True)
    res_plots = find_neighbours(res_plots, all_plots)
    logger.info('residential plot neighbors set.')

    res_plots['min_rect_edges'] = [get_edges(x) for x in [y.minimum_rotated_rectangle for y in res_plots.geometry]]
    edges = get_min_rect_edge_df(res_plots)
    intersection = intersect_roads_with_edges(edges, plots, res_plots)
    res_plots = set_min_rect_edge_types(intersection, edges, res_plots)
    res_plots = is_corner_plot(intersection, res_plots, 0.3)
    logger.info('corner plots set.')

    filtered_res_plots = res_plots[~res_plots['min_rect_front_edge'].isna()].set_index('plots')
    filtered_res_plots.loc[:, 'average_width'] = filtered_res_plots.apply(find_average_width_or_depth, axis=1,
                                                                          args=('average_width',)).to_numpy()
    filtered_res_plots.loc[:, 'average_depth'] = filtered_res_plots.apply(find_average_width_or_depth, axis=1,
                                                                          args=('average_depth',)).to_numpy()
    res_plots = res_plots.merge(filtered_res_plots.loc[:, ['average_width', 'average_depth']], how='left',
                                left_on='plots', right_index=True)
    logger.info('average plot width and depth set.')

    return res_plots


def set_road_plot_properties(road_network, road_plots):
    """
    Set road plot properties, such as road type and road category, based on the road network data.

    :param road_network: A GeoDataFrame containing road network data with attributes such as 'RD_TYP_CD'.
    :param road_plots: A GeoDataFrame containing road plot geometries and associated attributes.
    :return: A GeoDataFrame of road plots with additional properties including 'road_category'.
    """

    invalid_road_types = ['Cross Junction', 'T-Junction', 'Expunged', 'Other Junction', 'Pedestrian Mall',
                          '2 T-Junction opposite each other', 'Unknown', 'Y-Junction', 'Imaginary Line']
    road_network_valid = road_network[~road_network['RD_TYP_CD'].isin(invalid_road_types)]
    road_cat_dict = {'Expressway': '1',
                     'Semi Expressway': '2-3',
                     'Major Arterials/Minor Arterials': '2-3',
                     'Local Collector/Primary Access': '4',
                     'Local Access': '5',
                     'Slip Road': '5',
                     'Service Road': '5',
                     'no category': 'unknown'}
    road_plots_cat = assign_road_category(road_plots, road_network_valid.copy())
    road_plots_cat['road_category'] = [road_cat_dict[x] for x in road_plots_cat["RD_TYP_CD"]]
    logger.info('Road properties set.')

    return road_plots_cat


def assign_road_category(road_plots, road_network):
    """
    Overlap road network data with road plots and link road network attributes to the corresponding road plots.

    :param road_plots: A GeoDataFrame containing road plot geometries and identifiers.
    :param road_network: A GeoDataFrame containing road network data, including attributes such as 'RD_TYP_CD'.
    :return: A GeoDataFrame of road plots enriched with a 'RD_TYP_CD' column representing road categories.
    """

    road_network.loc[:, 'geometry'] = road_network.buffer(5)
    intersection = gpd.overlay(road_plots, road_network, how='intersection', keep_geom_type=True)
    intersection['intersection_area'] = intersection.area
    grouped_intersection = intersection.groupby(['plots']).apply(
        lambda x: x.sort_values(['intersection_area'], ascending=False).iloc[0, :]).drop(columns=['plots'])
    grouped_intersection = grouped_intersection.reset_index()
    grouped_intersection = grouped_intersection[['plots', 'RD_TYP_CD']].copy()
    road_plots = road_plots.merge(grouped_intersection, on='plots', how='left')
    road_plots.loc[road_plots["RD_TYP_CD"].isna(), "RD_TYP_CD"] = "no category"

    return road_plots


def find_allowed_residential_types(plots, road_list):
    """
    Determines the allowed residential development types for each plot based on zoning, area, dimensions,
    and regulatory conditions.

    :param plots: A DataFrame containing plot information including zone, area, width, depth, and regulatory attributes.
    :param road_list: A list of road types that influence allowed residential types based on plot neighbors.
    :return: A DataFrame with an added column 'allowed_residential_types' specifying the allowed residential types.
    """

    zone_list = ['Residential', 'ResidentialWithCommercialAtFirstStorey', 'CommercialAndResidential',
                 'ResidentialOrInstitution', 'White', 'BusinessParkWhite', 'Business1White', 'Business2White']
    mixed_zone_list = ['ResidentialWithCommercialAtFirstStorey', 'CommercialAndResidential', 'White',
                       'BusinessParkWhite', 'Business1White', 'Business2White']

    def determine_allowed_types(row):
        if row['zone'] not in zone_list:
            return []

        lha_programmes = row.get('lha_programmes', [])
        sbp_programmes = row.get('sbp_programmes', [])
        area, width, depth = row.get('plot_area', 0), row.get('avg_width', 0), row.get('avg_depth', 0)
        is_corner = row.get('corner_plot', 'false') == 'true'
        at_fringe = row.get('fringe_plot', 'false') == 'true'
        road_type = len(set(row.get('neighbour_road_type', [])).intersection(road_list)) > 0
        in_gcba = row.get('in_gcba', 0) > 0
        in_lha = bool(lha_programmes)
        allowed_programmes = sbp_programmes + lha_programmes

        allowed = []

        # Bungalow
        if area >= 400 and width >= 10 and (
                row['zone'] == 'Residential' or 'Bungalow' in allowed_programmes) and not in_gcba:
            allowed.append('Bungalow')

        # Semi-Detached House
        if area >= 200 and width >= 8 and (
                row['zone'] == 'Residential' or 'Semi-DetachedHouse' in allowed_programmes) and not in_gcba:
            allowed.append('Semi-DetachedHouse')

        # Terrace Type 1
        if (area >= 150 and width >= 6 and not is_corner or
            area >= 200 and width >= 8 and is_corner) and (
                row['zone'] == 'Residential' or 'TerraceHouse' in allowed_programmes or
                'TerraceType1' in allowed_programmes) and not in_gcba:
            allowed.append('TerraceType1')

        # Terrace Type 2
        if (area >= 80 and width >= 6 and not is_corner or
            area >= 80 and width >= 8 and is_corner) and (
                row['zone'] == 'Residential' or 'TerraceHouse' in allowed_programmes or
                'TerraceType2' in allowed_programmes) and not in_gcba:
            allowed.append('TerraceType2')

        # Good Class Bungalow
        if area >= 1400 and width >= 18.5 and depth >= 30 and (
                in_gcba or 'GoodClassBungalow' in sbp_programmes):
            allowed.append('GoodClassBungalow')

        # Flats, Condominiums, Serviced Apartments
        if area >= 1000 and row['zone'] in zone_list and not in_gcba and not in_lha:
            allowed.append('Flat')

        if area >= 4000 and row['zone'] in ['Residential', 'ResidentialOrInstitution'] and not in_gcba and not in_lha:
            allowed.append('Condominium')

        if row['zone'] == 'Residential' and not in_gcba and not in_lha and at_fringe and road_type:
            allowed.append('ServicedApartmentResidentialZone')

        if row['zone'] in mixed_zone_list and not in_gcba and not in_lha and at_fringe:
            allowed.append('ServicedApartmentMixedUseZone')

        return allowed

    plots['allowed_residential_types'] = plots.apply(determine_allowed_types, axis=1)
    return plots


def link_type_regulations_to_plots(regs, plots, road_list):
    """
    Links type-based regulations to plots based on specific conditions.

    :param regs: A DataFrame containing regulations with their attributes and conditions.
    :param plots: A DataFrame containing enriched plot data with attributes like zone, corner status, fringe status,
                  allowed residential types, and more.
    :param road_list: A list of road types that determine road-based regulations.
    :return: A DataFrame with regulations updated to include the list of plots they apply to.
    """

    # Pre-compute reusable conditions
    plots['in_central_area_flag'] = plots['in_central_area'] > 0
    plots['fringe_plot_flag'] = plots['fringe_plot'] == 'true'
    plots['corner_plot_flag'] = plots['corner_plot'] == 'true'

    def has_intersection(list1, list2):
        return bool(set(list1).intersection(list2))

    reg_plots = []

    for _, reg in regs.iterrows():

        applies_to = plots['zone'].isin(reg['for_zones'])

        # Handle programme-based conditions
        programmes = ['Semi-DetachedHouse', 'Bungalow', 'TerraceType1', 'TerraceType2', 'GoodClassBungalow']
        if reg['for_programme'] in programmes:
            applies_to &= plots['allowed_residential_types'].apply(lambda types: reg['for_programme'] in types)

        if reg['for_programme'] in ['Flat', 'Condominium']:
            if not pd.isna(reg['gpr_function']):
                applies_to &= plots['gpr'] > reg['requires_gpr']
            else:
                applies_to &= plots['gpr'] == reg['requires_gpr']

        # Handle central area conditions
        if reg['in_central_area'] == 'true':
            applies_to &= plots['in_central_area_flag']
        elif reg['in_central_area'] == 'false':
            applies_to &= ~plots['in_central_area_flag']

        # Handle area regulation conditions
        if reg['in_area_regs']:
            applies_to &= plots['in_pb'].apply(lambda pb: has_intersection(pb, reg['in_area_regs'])) | \
                          plots['in_lha'].apply(lambda lha: has_intersection(lha, reg['in_area_regs']))

        # Handle fringe and corner plots
        if reg['for_fringe_plot']:
            applies_to &= plots['fringe_plot_flag']
        if reg['for_corner_plot']:
            applies_to &= plots['corner_plot_flag']

        # Handle road conditions
        if reg['abuts_road']:
            applies_to &= plots['neighbour_road_type'].apply(lambda road_types: has_intersection(road_types, road_list))

        # Handle GCBA conditions
        if reg['abuts_gcba'] == 'true':
            applies_to &= plots['abuts_gcba'].fillna(0) > 0
        if reg['in_gcba'] == 'true':
            applies_to &= plots['in_gcba'].fillna(0) > 0

        # Handle neighbour zone conditions
        if len(reg['neighbour_zones']) > 1:
            applies_to &= plots['neighbour_zones'].apply(lambda zones: has_intersection(zones, reg['neighbour_zones']))

        # Store the matching plots
        reg_plots.append(plots.loc[applies_to, 'plots'].tolist())

    regs['applies_to'] = reg_plots
    return regs


def get_context_gpr(neighbours, gpr_map):
    """
    Efficiently calculates the mean GPR of the given neighbours using a precomputed mapping.

    :param neighbours: List of neighbouring plot IDs.
    :param gpr_map: A Series mapping plot IDs to their GPR values.
    :return: Rounded mean GPR value of the neighbours or NaN if no valid GPRs are found.
    """
    neighbour_gprs = gpr_map.reindex(neighbours).dropna()

    return round(neighbour_gprs.mean(), 1) if not neighbour_gprs.empty else np.nan


def assign_zone_gpr(plots,
                    zone_type,
                    lha,
                    reg_links,
                    in_lha_gpr,
                    fringe_gpr,
                    in_context_gpr,
                    fringe_storeys,
                    context_storeys):
    """
    The function writes missing GPR values for educational, religious and civic plots.
    Look up: https://www.ura.gov.sg/Corporate/Guidelines/Development-Control/Non-Residential/EI/GPR-Building-Height

    :param plots: Singapore's Masterplan 2019 plots GeoDataFrame queried from the KG.
    :param zone_type: plot zoning type for which GPRs are reset.
    :param lha: a GeoDataFrame with LandedHousingArea planning regulations.
    :param reg_links: a DataFrame containing plot ids, regulation ids, and regulation type.
    :param in_lha_gpr: a GPR value for case when plot is within landed housing area.
    :param fringe_gpr: a GPR value for case when plot is at residential fringe.
    :param in_context_gpr: a GPR value for case when plot is in relevant to the zone type context.
    :param fringe_storeys: number of storeys for the case when plot is at residential fringe.
    :param context_storeys: number of storeys for the case when plot is in relevant to the zone type context.
    :return: a modified plots GeoDataFrame with GPR values added for the plots with relevant zoning type.
    """
    lha_names = {'LandedHousingArea', 'GoodClassBungalowArea'}
    industrial_zones = {'Business1', 'Business2', 'BusinessPark'}

    target_plots = plots[plots['zone'] == zone_type]

    reg_links_map = reg_links.groupby('plots')
    neighbour_map = plots.set_index('plots')['neighbour']
    gpr_map = plots.set_index('plots')['gpr']
    plots['gpr'] = plots.get('gpr', np.nan)
    plots['context_storeys'] = plots.get('context_storeys', np.nan)

    for i, row in target_plots.iterrows():
        neighbours = neighbour_map.get(row['plots'], [])
        context_gpr = get_context_gpr(neighbours, gpr_map)
        plot_regs = reg_links_map.get_group(row['plots']) if row['plots'] in reg_links_map.groups else pd.DataFrame()
        neighbour_regs = reg_links[reg_links['plots'].isin(neighbours)]

        has_industrial_neighbours = bool(set(row['neighbour_zones']).intersection(industrial_zones))
        in_ca = any(plot_regs['reg_type'].isin(['CentralArea']))
        in_lha = any(plot_regs['reg_type'].isin(lha_names))
        in_lha_fringe = any(neighbour_regs['reg_type'].isin(lha_names))
        in_industrial_zone = any(target_plots['zone'].isin(industrial_zones))

        if in_lha and (context_gpr <= 1.4):
            lha_id = plot_regs.loc[plot_regs['reg_type'].isin(lha_names), 'reg']
            lha_storeys = int(lha.loc[lha['reg'].isin(lha_id), 'storeys'].values[0])
            plots.loc[i, 'gpr'] = in_lha_gpr
            plots.loc[i, 'context_storeys'] = lha_storeys if not lha_id.empty else np.nan
        elif in_lha_fringe and (context_gpr <= 1.4):
            plots.loc[i, 'gpr'] = fringe_gpr
            plots.loc[i, 'context_storeys'] = fringe_storeys
        elif (context_gpr > 1.4) or has_industrial_neighbours or in_industrial_zone:
            plots.loc[i, 'gpr'] = in_context_gpr
            plots.loc[i, 'context_storeys'] = context_storeys
        elif in_ca:
            plots.loc[i, 'gpr'] = np.nan

    return plots


def assign_sbp_gpr(plots, sbp, reg_links):
    """
    The function writes missing or updates existing GPR values for plots contained in street block plans.

    :param plots: Singapore's Masterplan 2019 plots GeoDataFrame queried from the KG.
    :param sbp: a GeoDataFrame containing StreetBlockPlans planning regulations.
    :param reg_links: a DataFrame containing plot ids, regulation ids, and regulation type.
    :return: a modified plots GeoDataFrame with GPR values added or modified for relevant plots.
    """

    sbp_plot_ids = set(reg_links[reg_links['reg_type'] == 'StreetBlockPlan']['plots'])
    target_plots = plots[plots['plots'].isin(list(sbp_plot_ids))].copy()
    reg_links_map = reg_links.groupby('plots')

    for i, row in target_plots.iterrows():
        cur_gpr = float(row['gpr'])
        cur_regs = reg_links_map.get_group(row['plots']) if row['plots'] in reg_links_map.groups else pd.DataFrame()
        sbp_id = cur_regs.loc[cur_regs['reg_type'] == 'StreetBlockPlan', 'reg']
        sbp_gpr = float(sbp.loc[sbp['reg'].isin(list(sbp_id)), 'gpr'].sample())

        if not pd.isna(sbp_gpr) and pd.isna(cur_gpr):
            plots.loc[i, 'gpr'] = sbp_gpr
        elif (not pd.isna(sbp_gpr)) and (not pd.isna(cur_gpr)):
            plots.loc[i, 'gpr'] = min(sbp_gpr, cur_gpr)

    return plots


def assign_gpr(plots, lha, sbp, reg_links):
    """

    :param plots:
    :param lha:
    :param sbp:
    :param reg_links:
    :return:
    """
    plots = assign_zone_gpr(plots, 'PlaceOfWorship', lha, reg_links, 1., 1.4, 1.6, 4, 5)
    plots = assign_zone_gpr(plots, 'EducationalInstitution', lha, reg_links, 1., 1., 1.4, 3, 4)
    plots = assign_zone_gpr(plots, 'CivicAndCommunityInstitutionZone', lha, reg_links, 1., 1.4, 1.4, 3, 4)
    plots = assign_sbp_gpr(plots, sbp, reg_links)

    return plots


def set_partywall_plots(plots, reg_links, sbp, udg):
    """
    The function adds a boolean value for partywall plots based on applicable planning regulations.

    :param plots: Singapore's Masterplan 2019 plots GeoDataFrame queried from the KG.
    :param reg_links: DataFrame containing plot ids, regulation ids, and regulation type.
    :param sbp: a GeoDataFrame containing StreetBlockPlans planning regulations.
    :param udg: a GeoDataFrame containing UrbanDesignGuideline planning regulations.
    :return: a modified plots GeoDataFrame with a column 'partywall'.
    """

    sbp_regs = sbp.loc[sbp['setback_type'] == 'PartyWall', 'reg']
    sbp_plots = reg_links.loc[reg_links['reg'].isin(sbp_regs), 'plots']
    udg_regs = udg.loc[udg['partywall'] == 'true', 'reg']
    udg_plots = reg_links.loc[reg_links['reg'].isin(udg_regs), 'plots']

    party_wall_types = {'TerraceType1', 'TerraceType2', 'Semi-DetachedHouse'}
    res_plots = plots.loc[
        plots['allowed_residential_types'].apply(lambda x: bool(set(x).intersection(party_wall_types))), 'plots']

    partywall_plots = pd.concat([sbp_plots, udg_plots, res_plots]).unique()
    plots['partywall'] = plots['plots'].isin(partywall_plots)

    return plots


def get_unclear_plots(plots, reg_links, hcp, udg):
    """
    Generates a list of unique plot IDs that do not qualify for GFA calculation.

    :param plots: GeoDataFrame containing plot data.
    :param reg_links: DataFrame containing plot IDs, regulation IDs, and regulation type.
    :param hcp: GeoDataFrame containing HeightControlPlan planning regulations.
    :param udg: GeoDataFrame containing UrbanDesignGuideline planning regulations.
    :return: A list of unique unclear plot IDs.
    """

    non_gfa_zones = ['Road', 'Waterbody', 'Utility', 'OpenSpace', 'ReserveSite', 'Park', 'Agriculture',
                     'MasRapidTransit', 'RapidTransit', 'PortOrAirport', 'SpecialUseZone', 'Cemetery',
                     'BeachArea', 'LightRapidTransit']

    con_plots = set(reg_links.loc[reg_links['reg_type'].isin(['ConservationArea', 'Monument']), 'plots'])

    hcp_reg_ids = set(hcp.loc[~hcp['additional_type'].isna(), 'reg'])
    hcp_plots = set(reg_links.loc[reg_links['reg'].isin(hcp_reg_ids), 'plots'])

    udg_reg_ids = set(udg.loc[~udg['additional_type'].isna(), 'reg'])
    udg_plots = set(reg_links.loc[reg_links['reg'].isin(udg_reg_ids), 'plots'])

    non_gfa_zone_plots = set(plots.loc[plots['zone'].isin(non_gfa_zones), 'plots'])

    unclear_plots = con_plots | hcp_plots | udg_plots | non_gfa_zone_plots

    return list(unclear_plots)


def get_plot_edges(plots):
    """
    The function modifies the plot dataframe with a new 'edges' column and generates a GeoDataFrame with all plot edges.

    :param plots: Singapore's Masterplan 2019 plots GeoDataFrame queried from the KG.
    :return: A modified plots GeoDataFrame and a GeoDataFrame with all plots' edges.
    """

    plots['edges'] = plots['geometry'].apply(get_edges)
    edges = plots[['partywall', 'plots', 'edges', 'geometry']].explode(column='edges').drop(['geometry'], axis=1)

    edges = edges.set_geometry('edges', crs=3857)
    edges['geometry'] = edges.geometry.buffer(1, single_sided=True)
    edges['buffered_edge_area'] = edges['geometry'].area
    edges['edge_index'] = edges.groupby('plots').cumcount()

    return plots, edges


def get_min_rect_edges(sbp_plots):
    """
    The function modifies the plots dataset and generates all plots' minimum bounding rectangle edge dataframe.

    :param sbp_plots: Singapore's Masterplan 2019 plots GeoDataFrame queried from the KG.
    :return: A modified plots GeoDataFrame and a GeoDataFrame with every plot's minimum bounding rectangle edges.
    """
    sbp_plots = sbp_plots.copy()
    sbp_plots['min_rect_edges'] = sbp_plots.geometry.apply(lambda geom: get_edges(geom.minimum_rotated_rectangle))
    min_rect_edges = sbp_plots[['plots', 'min_rect_edges']].explode(column='min_rect_edges')
    min_rect_edges = min_rect_edges.set_geometry('min_rect_edges', crs=3857)
    min_rect_edges.geometry = min_rect_edges.buffer(-3, single_sided=True)
    min_rect_edges['length'] = min_rect_edges.geometry.length.round(decimals=3)
    min_rect_edges['min_rect_edge_index'] = min_rect_edges.groupby(level=0).cumcount()

    return sbp_plots, min_rect_edges


def classify_min_rect_edges(min_rect_edge_df, plots, roads):
    """
    The function classifies minimum bounding rectangle edges into front, side, and rear.
    The function first identifies the front edge - an edge that overlaps most with the road.
    Other edges can be interpolated from there on.
    Applicable only to min_rect_edges of plots that are in StreetBlockPlans.

    :param min_rect_edge_df: GeoDataFrame containing every plot's every edge.
    :param plots: Singapore's Masterplan 2019 plots GeoDataFrame queried from the KG.
    :param roads: plots GeoDataFrame which has zoning type 'Road'.
    :return: A modified plots GeoDataFrame with columns storing indexes of categorized minimum bounding rectangle edges.
    """

    # Perform overlay to find intersections
    intersection = gpd.overlay(min_rect_edge_df, roads, how='intersection', keep_geom_type=True)
    intersection['intersection_area'] = intersection.area

    # Sort edges and intersections for consistency
    min_rect_edge_df = min_rect_edge_df.sort_values('plots')
    intersection = intersection.sort_values(by=['plots_1', 'intersection_area', 'length'], ascending=False)

    # Determine front edge indices
    front_edge = intersection.groupby('plots_1')['min_rect_edge_index'].first()
    plots = plots.merge(front_edge.rename('min_rect_front_edge'), left_on='plots', right_index=True, how='left')

    # Identify non-front edges
    front_edge_map = plots.set_index('plots')['min_rect_front_edge']
    not_front_edge = min_rect_edge_df['min_rect_edge_index'] != min_rect_edge_df['plots'].map(front_edge_map)

    # Compute front edge lengths, handling missing front edges
    front_edge_length = min_rect_edge_df.loc[~not_front_edge, ['plots', 'length']].set_index('plots')['length']
    missing_front_plots = set(min_rect_edge_df['plots'].unique()) - set(front_edge_length.index)

    front_edge_length = pd.concat(
        [front_edge_length, pd.Series([0] * len(missing_front_plots), index=missing_front_plots)])
    front_edge_length = min_rect_edge_df['plots'].map(front_edge_length).fillna(0)

    # Identify rear edges
    rear_edge = not_front_edge & (min_rect_edge_df['length'] == front_edge_length)
    rear_edge_indices = min_rect_edge_df.loc[rear_edge].groupby('plots')['min_rect_edge_index'].first()
    plots = plots.merge(rear_edge_indices.rename('min_rect_rear_edge'), left_on='plots', right_index=True, how='left')

    # Compute side edges
    front_rear_map = plots[['min_rect_front_edge', 'min_rect_rear_edge']].to_dict(orient='index')
    side_edges = []
    for idx in plots.index:
        front = front_rear_map[idx]['min_rect_front_edge']
        rear = front_rear_map[idx]['min_rect_rear_edge']
        side_edges.append([e for e in {0.0, 1.0, 2.0, 3.0} if e not in {front, rear}])

    plots['min_rect_side_edges'] = side_edges
    plots.loc[plots['min_rect_front_edge'].isna(), 'min_rect_side_edges'] = np.nan

    return plots


def classify_neighbours(sbp_plots, plots, min_rect_edge_df):
    """
    The function classifies neighbours based on overlap with corresponding classified minimum bounding rectangle edges.
    Applicable only to plots that are in StreetBlockPlans.

    :param sbp_plots: filtered plots GeoDataFrame for plots that fall in Street Block Plan areas.
    :param plots: Singapore's Masterplan 2019 plots GeoDataFrame queried from the KG.
    :param min_rect_edge_df: GeoDataFrame containing minimum bounding rectangle edges.
    :return: A modified GeoDataFrame of sbp_plots with neighbour ids categorised and stored in new relevant columns.
    """

    for col in ['side_neighbours', 'front_neighbours', 'rear_neighbours']:
        sbp_plots[col] = [[] for _ in sbp_plots.index]

    for i in sbp_plots[sbp_plots['min_rect_front_edge'].notnull()].index:
        cur_plot_id = sbp_plots.loc[i, 'plots']
        cur_neighbours = plots[plots['plots'].isin(sbp_plots.loc[i, 'neighbour'])][['plots', 'geometry']]

        cur_edges = min_rect_edge_df[min_rect_edge_df['plots'] == cur_plot_id][
            ['min_rect_edges', 'min_rect_edge_index']].set_index('min_rect_edge_index').copy()
        cur_edges['edge_type'] = 'side'
        cur_edges.loc[int(sbp_plots.loc[i, 'min_rect_front_edge']), 'edge_type'] = 'front'
        cur_edges.loc[int(sbp_plots.loc[i, 'min_rect_rear_edge']), 'edge_type'] = 'rear'

        neighbour_int = gpd.sjoin(cur_neighbours, cur_edges, op='intersects', rsuffix='edge')
        neighbour_int['geometry'] = neighbour_int.geometry.intersection(
            cur_edges.loc[neighbour_int['index_edge'], :].geometry,
            align=False
        )
        neighbour_int.set_geometry('geometry', inplace=True)
        neighbour_int['intersection_area'] = neighbour_int.geometry.area

        neighbour_types = (
            neighbour_int.sort_values(by=['plots', 'intersection_area'], ascending=False)
            .groupby('plots')['edge_type']
            .first()
            .reset_index()
        )

        sbp_plots.at[i, 'side_neighbours'] = list(neighbour_types[neighbour_types['edge_type'] == 'side']['plots'])
        sbp_plots.at[i, 'front_neighbours'] = list(neighbour_types[neighbour_types['edge_type'] == 'front']['plots'])
        sbp_plots.at[i, 'rear_neighbours'] = list(neighbour_types[neighbour_types['edge_type'] == 'rear']['plots'])

    return sbp_plots


def classify_street_block_plan_plots(gfa_plots, plots, reg_links, road_plots):
    sbp_plots_ids = list(reg_links[reg_links['reg_type'] == 'StreetBlockPlan']['plots'].unique())
    sbp_plots = gfa_plots[gfa_plots['plots'].isin(sbp_plots_ids)]
    sbp_plots, min_rect_edge_df = get_min_rect_edges(sbp_plots)
    sbp_plots = classify_min_rect_edges(min_rect_edge_df, sbp_plots.copy(), road_plots)
    sbp_plots = classify_neighbours(sbp_plots.copy(), plots, min_rect_edge_df)

    return sbp_plots


def classify_plot_edges(gfa_plots, sbp_plots, plots, edges):
    """
    The function classifies plot edges into front, side and rear based on overlap with classified neighbours.
    Applicable only to plots that are in StreetBlockPlans.

    :param sbp_plots: filtered plots GeoDataFrame for plots that fall in Street Block Plan areas.
    :param gfa_plots: filtered plots GeoDataFrame for which gfa should be estimated.
    :param plots: Singapore's Masterplan 2019 plots GeoDataFrame queried from the KG.
    :param edges: a GeoDataFrame containing every plots every edge.
    :return: a modified plots GeoDataFrame with plot edges classified and edge indexes stored in relevant columns.
    """

    for col in ['side_edges', 'front_edges', 'rear_edges']:
        gfa_plots[col] = [[] for _ in gfa_plots.index]

    for i in sbp_plots.index:
        cur_plot_id = gfa_plots.loc[i, 'plots']
        cur_neighbours_ids = gfa_plots.loc[i, 'neighbour']
        cur_neighbours = plots.loc[plots['plots'].isin(cur_neighbours_ids), ['plots', 'geometry']].rename(
            columns={'plots': 'neighbours'}
        )
        cur_plot_edges = edges[edges['plots'] == cur_plot_id].reset_index().set_geometry('geometry')
        cur_plot_edges['index_edge'] = cur_plot_edges.index

        edge_int = gpd.sjoin(cur_neighbours, cur_plot_edges, op='intersects', rsuffix='neighbour')
        edge_int['geometry'] = edge_int.apply(
            lambda row: row['geometry'].intersection(
                cur_plot_edges.loc[row['index_edge'], 'geometry']
            ),
            axis=1
        )
        edge_int.set_geometry('geometry', inplace=True)
        edge_int['intersection_area'] = edge_int.geometry.area
        edge_int = (
            edge_int.sort_values(by=['index_edge', 'intersection_area'], ascending=False)
            .groupby('index_edge')
            .first()
            .reset_index()
        )

        side_edges = edge_int[edge_int['neighbours'].isin(sbp_plots.loc[i, 'side_neighbours'])]['index_edge']
        front_edges = edge_int[edge_int['neighbours'].isin(sbp_plots.loc[i, 'front_neighbours'])]['index_edge']
        rear_edges = edge_int[edge_int['neighbours'].isin(sbp_plots.loc[i, 'rear_neighbours'])]['index_edge']

        gfa_plots.at[i, 'side_edges'].extend(side_edges)
        gfa_plots.at[i, 'front_edges'].extend(front_edges)
        gfa_plots.at[i, 'rear_edges'].extend(rear_edges)

    return gfa_plots


def set_road_buffer_edges(edges, plots, gfa_plots):
    """
    The function identifies plot edge indexes that will be subject to relevant road buffers.

    :param edges: a GeoDataFrame containing every plots every edge.
    :param plots: Singapore's Masterplan 2019 plots GeoDataFrame queried from the KG.
    :param gfa_plots: filtered plots GeoDataFrame for which gfa should be estimated.
    :return: a modified plots GeoDataFrame with new columns 'cat_1_edges', 'cat_2_edges and 'cat_3_5_edges',
    containing classified plot edge indexes.
    """

    edges = edges.set_geometry('geometry')
    edges_int = gpd.overlay(edges, plots[['geometry', 'road_type']], how='intersection', keep_geom_type=False)
    edges_int['intersection_area'] = edges_int.area
    edges_int = edges_int[(edges_int['intersection_area'] / edges_int['buffered_edge_area']) > 0.2]

    # this mapping is our interpretation of the regulations. There are no explicit mapping by the agencies.
    road_cats = {'cat_1_edges': ['Expressway', 'Semi Expressway'],
                 'cat_2_edges': ['Major Arterials/Minor Arterials'],
                 'cat_3_5_edges': ['Local Access', 'Local Collector/Primary Access', 'Slip Road', 'Service Road'],
                 'no_cat_edges': ['no category']}

    for category, road_type in road_cats.items():
        cat = edges_int[edges_int['road_type'].isin(road_type)]
        cat_edges = cat.groupby(by='plots')['edge_index'].apply(lambda e: list(set(e))).rename(category)
        gfa_plots = gfa_plots.merge(cat_edges, left_on='plots', right_index=True, how='left')
        gfa_plots[category] = gfa_plots[category].fillna('').apply(list)

    return gfa_plots


def set_partywall_edges(edges, plots, gfa_plots):
    """
    The function specifies plot edge indexes that are partywall edges (0m buffer)
    based on buffered partywall plot edge and neighbouring partywall plot intersections.

    :param edges: a GeoDataFrame containing every plots every edge.
    :param plots: Singapore's Masterplan 2019 plots GeoDataFrame queried from the KG.
    :param gfa_plots: filtered plots GeoDataFrame for which gfa should be estimated.
    :return: a modified plots GeoDataFrame with a new column 'partywall_edges', containing a list of edge indexes.
    """

    partywall_edges_df = edges[edges['partywall']].set_geometry('geometry')
    partywall_plots = plots[plots['partywall']][['geometry']]

    edges_int = gpd.overlay(partywall_edges_df, partywall_plots, how='intersection', keep_geom_type=False)
    edges_int['intersection_area'] = edges_int.area
    edges_int = edges_int.loc[(edges_int['intersection_area'] / edges_int['buffered_edge_area']) > 0.5]

    partywall_edges = edges_int.groupby('plots')['edge_index'].agg(set).rename('partywall_edges')

    gfa_plots = gfa_plots.merge(partywall_edges, left_on='plots', right_index=True, how='left')
    gfa_plots['partywall_edges'] = gfa_plots['partywall_edges'].apply(lambda x: list(x) if isinstance(x, set) else [])
    gfa_plots['partywall_edges'] = gfa_plots.apply(
        lambda row: list(set(row['partywall_edges']) - set(row['rear_edges'])), axis=1
    )

    return gfa_plots


def get_udg_edge_setbacks(udg, reg_links, edges):
    """
    The function generates a DataFrame with plot ids and edge indexes to which UDG for setbacks apply.

    :param udg: a GeoDataFrame containing UrbanDesignGuideline regulation content
    :param reg_links: a DataFrame containing plot ids, regulation ids, and regulation type.
    :param edges: a GeoDataFrame containing every plots every edge.
    :return: a DataFrame containing plot edge indexes and applicable udg setbacks.
    """

    edges = edges.set_geometry('geometry')
    udg_with_setback = udg[~udg['setback'].isna()][['reg', 'setback', 'geometry']]
    valid_udg_regs = reg_links['reg'].unique()
    udg_with_setback = udg_with_setback[udg_with_setback['reg'].isin(valid_udg_regs)]

    edges_int = gpd.overlay(edges, udg_with_setback, how='intersection', keep_geom_type=False)
    edges_int['intersection_area'] = edges_int.area
    edges_int = edges_int[(edges_int['intersection_area'] / edges_int['buffered_edge_area']) > 0.5]

    valid_links = reg_links[reg_links['reg'].isin(valid_udg_regs)]
    edges_int = edges_int.merge(valid_links, on='plots', how='left', suffixes=(None, '_links'))

    edges_int = edges_int[
        (~edges_int['reg_links'].isna()) & (edges_int['reg_links'] == edges_int['reg'])
        ].drop(columns=['reg_type'])

    udg_edges = (
        edges_int.sort_values(by=['plots', 'edge_index', 'intersection_area'], ascending=False)
        .groupby(['plots', 'edge_index'], as_index=False)
        .first()
        .drop(columns=['partywall', 'buffered_edge_area', 'intersection_area', 'reg_links'])
    )

    return udg_edges


def set_plot_edge_setbacks(gfa_plots, reg_links, dcp, sbp, road_cats, udg_edges, residential_zones):
    """
    The function updates every plot edge setback value based on applicable regulations and saves edge setback list.

    :param residential_zones: list of residential zoning types.
    :param gfa_plots: filtered plots GeoDataFrame for which gfa should be estimated.
    :param reg_links: a DataFrame containing plot ids, regulation ids, and regulation type.
    :param dcp: a DataFrame containing DevelopmentControlPlan regulation content.
    :param sbp: a GeoDataFrame containing StreetBlockPlan regulation content.
    :param road_cats: a DataFrame containing road category regulation content.
    :param udg_edges: a DataFrame containing plot edge indexes and applicable udg setbacks.
    :return: a modified plot GeoDataFrame with a list of plot edge setbacks stored in a column 'edge_setbacks'.
    """

    all_setbacks = []

    reg_links_by_plot = reg_links.groupby('plots')
    sbp_by_reg = sbp.groupby('reg')
    road_cats_by_reg = road_cats.groupby('road_reg')
    udg_edges_by_plot = udg_edges.groupby('plots')

    for count, i in enumerate(gfa_plots.index):
        plot_setbacks = {}
        plot_id = gfa_plots.at[i, 'plots']

        if plot_id in reg_links_by_plot.groups:
            num_of_edges = len(gfa_plots.at[i, 'edges'])
            cur_regs = reg_links_by_plot.get_group(plot_id)
            plot_zone = gfa_plots.at[i, 'zone']

            cur_dcp = dcp[dcp['reg'].isin(cur_regs[cur_regs['reg_type'] == 'DevelopmentControlPlan']['reg'])]
            if plot_zone in residential_zones:
                allowed_res_programmes = set(gfa_plots.at[i, 'allowed_residential_types'] + ['Clinic'])
                cur_dcp = cur_dcp[cur_dcp['programme'].isin(allowed_res_programmes)]

            sbp_setbacks = {'FrontSetback': [], 'SideSetback': [], 'RearSetback': []}

            if 'StreetBlockPlan' in cur_regs['reg_type'].unique():
                relevant_sbp_regs = cur_regs[cur_regs['reg_type'] == 'StreetBlockPlan']['reg']
                cur_sbp = pd.concat(
                    [sbp_by_reg.get_group(reg) for reg in relevant_sbp_regs if reg in sbp_by_reg.groups])
                for setback_type in sbp_setbacks.keys():
                    sbp_setbacks[setback_type] = list(
                        cur_sbp[cur_sbp['setback_type'] == setback_type].sort_values(by='level')['setback']
                    )

            road_category_groups = [
                road_cats_by_reg.get_group(reg)
                for reg in cur_dcp['road_categories'].explode().dropna().unique()
                if reg in road_cats_by_reg.groups
            ]
            cur_road_cats = pd.concat(road_category_groups,
                                      ignore_index=True) if road_category_groups else pd.DataFrame()
            udg_setbacks = udg_edges_by_plot.get_group(
                plot_id) if plot_id in udg_edges_by_plot.groups else pd.DataFrame()

            for reg in cur_dcp.index:
                programme = cur_dcp.at[reg, 'programme']
                setback_list = [float(cur_dcp.at[reg, 'setback'])] * num_of_edges

                if not udg_setbacks.empty:
                    setback_map = udg_setbacks.set_index('edge_index')['setback']
                    for edge, setback in setback_map.items():
                        setback_list[edge] = max(setback_list[edge], setback)

                for edge_type, edge_indices in zip(['front_edges', 'side_edges', 'rear_edges'], sbp_setbacks.values()):
                    for edge in gfa_plots.at[i, edge_type]:
                        setback_list[edge] = max(setback_list[edge],
                                                 edge_indices[0] if edge_indices else setback_list[edge])

                for category, edges_key in zip([1, 2, 3], ['cat_1_edges', 'cat_2_edges', 'cat_3_5_edges']):
                    if gfa_plots.at[i, edges_key] and not cur_road_cats.empty:
                        max_buffer = cur_road_cats[cur_road_cats['category'] == category]['buffer'].max()
                        if pd.notna(max_buffer):
                            for edge in gfa_plots.at[i, edges_key]:
                                setback_list[edge] = max(setback_list[edge], max_buffer)

                partywall_edges = gfa_plots.at[i, 'partywall_edges'] if gfa_plots.at[i, 'partywall'] else []

                if programme in ['Semi-DetachedHouse', 'TerraceType1', 'TerraceType2']:
                    partywall_edges = list(set(partywall_edges + gfa_plots.at[i, 'side_edges']))
                if programme in ['Bungalow', 'GoodClassBungalow']:
                    partywall_edges = []

                for edge in partywall_edges:
                    setback_list[edge] = 0.

                plot_setbacks[programme] = setback_list

        all_setbacks.append(plot_setbacks)
        sys.stdout.write("{:d}/{:d} plots processed\r".format(count + 1, gfa_plots.shape[0]))
    sys.stdout.write("{:d}/{:d} plots processed\n".format(count + 1, gfa_plots.shape[0]))

    gfa_plots['edge_setbacks'] = all_setbacks

    return gfa_plots


def create_setback_area(edges, edge_setbacks, plot_geom):
    """
    Function to buffer edges with associated setbacks, merge polygons,
    and subtract the buffered geometry from the plot geometry.

    :param edges: a GeoDataFrame containing every plot's edges.
    :param edge_setbacks: a list with buffer values for every edge of a plot geometry.
    :param plot_geom: plot geometry as a Shapely Polygon.
    :return: remaining geometry after subtracting buffered edge geometry from the plot geometry.
    """
    edges_buffered = [
        edge.buffer(-setback, single_sided=True, cap_style=3, join_style=3)
        for edge, setback in zip(edges, edge_setbacks) if setback > 0
    ]

    buffered_area = unary_union(edges_buffered) if edges_buffered else Polygon()
    remaining_area = plot_geom.difference(buffered_area)

    return remaining_area


def get_buildable_footprints(gfa_plots):
    """
    Function to generate a list of buildable footprints for every unique/known setback storey.
    Position in the list indicates at which floor that footprint exists.

    :param gfa_plots: filtered plots GeoDataFrame for which GFA should be estimated.
    :return: a modified plots GeoDataFrame with a list of footprints.
    """
    all_setbacked_geom = []

    for count, plot in enumerate(gfa_plots.index):
        setbacked_geom = {}
        plot_edges = gfa_plots.at[plot, 'edges']
        plot_setbacks = gfa_plots.at[plot, 'edge_setbacks']

        if plot_setbacks:
            for programme, cur_setback in plot_setbacks.items():

                cur_setback = [
                    [setback] if not isinstance(setback, list) else setback
                    for setback in cur_setback
                ]

                num_of_levels = max(map(len, cur_setback))
                setbacked_geom[programme] = []

                for level in range(num_of_levels):
                    level_setback = [
                        float(cur_setback[j][level]) if len(cur_setback[j]) > level
                        else float(cur_setback[j][-1])
                        for j in range(len(cur_setback))
                    ]

                    remaining_geom = create_setback_area(plot_edges, level_setback, gfa_plots.at[plot, 'geometry'])

                    if remaining_geom.area > 0:
                        setbacked_geom[programme].append(remaining_geom)

        all_setbacked_geom.append(setbacked_geom)

        if count % 10 == 0 or count == len(gfa_plots) - 1:
            sys.stdout.write(f"{count + 1}/{len(gfa_plots)} plots processed\r")
    sys.stdout.write(f"{len(gfa_plots)}/{len(gfa_plots)} plots processed\n")

    gfa_plots['footprints'] = all_setbacked_geom

    return gfa_plots


def get_plot_parts(plot, cur_reg_type, storey_regs, plots):
    """
    Generates a list of plot parts based on planning regulations.

    :param plot: Index of the plot for which parts are generated.
    :param cur_reg_type: Regulation type for which current parts are retrieved.
    :param storey_regs: GeoDataFrame containing regulations applicable to specific floors.
    :param plots: GeoDataFrame containing the filtered plots.
    :return: A tuple containing:
        - List of used regulation IDs.
        - List of storeys corresponding to each part.
        - List of geometries for each part.
    """

    cur_reg = storey_regs.loc[storey_regs['reg_type'] == cur_reg_type, ['reg', 'storeys', 'geometry']].copy()
    if cur_reg.empty:
        return [], [], [plots.at[plot, 'geometry']]

    cur_reg = gpd.GeoDataFrame(cur_reg, geometry='geometry', crs=3857)
    plot_geom = gpd.GeoDataFrame({'geometry': [plots.at[plot, 'geometry']]}, geometry='geometry', crs=3857)
    reg_parts = gpd.overlay(cur_reg, plot_geom, how='intersection', keep_geom_type=False)

    used_regs = cur_reg['reg'].tolist()
    parts = list(reg_parts['geometry'])
    storeys = list(reg_parts['storeys'])

    reg_union = unary_union(parts)
    remaining_part = plots.loc[plot, 'geometry'].difference(reg_union)

    if remaining_part.is_empty:
        return used_regs, storeys, parts

    if remaining_part.area / plots.loc[plot, 'geometry'].area > 0.1:
        remaining_part = remaining_part.buffer(-1, join_style=2).buffer(1, join_style=2)
        parts.append(remaining_part)
        storeys.append(float('inf'))

    return used_regs, storeys, parts


def get_buildable_storeys(gfa_plots, udg, hcp, dcp, lha, sbp, reg_links, residential_zones):
    """
    Estimates the allowed number of storeys for each plot or plot part.

    :param gfa_plots: GeoDataFrame containing filtered plots for which GFA is estimated.
    :param udg: GeoDataFrame containing UrbanDesignGuideline regulation content.
    :param hcp: GeoDataFrame containing HeightControlPlan regulation content.
    :param dcp: DataFrame containing DevelopmentControlPlan regulation content.
    :param lha: GeoDataFrame containing LandedHousingArea regulation content.
    :param sbp: GeoDataFrame containing StreetBlockPlan regulation content.
    :param residential_zones: list of residential zoning types.
    :param reg_links: DataFrame containing plot IDs, regulation IDs, and regulation types.
    :return: A GeoDataFrame with added columns for storeys and plot parts.
    """
    combined_storeys = []
    combined_parts = []
    hcp_udg = pd.concat([hcp[['reg', 'storeys', 'abs_height', 'geometry']], udg[['reg', 'storeys', 'geometry']]])
    height_regs = reg_links.merge(hcp_udg, on='reg', how='left')
    height_regs = height_regs[(~height_regs['storeys'].isna()) | (~height_regs['abs_height'].isna())]
    height_regs = height_regs.groupby('plots')

    reg_links_by_plot = reg_links.groupby('plots')
    storey_height_map = {zone: 3.6 for zone in residential_zones}
    storey_height_map['default'] = 5.0

    for count, plot_index in enumerate(gfa_plots.index):
        plot_id = gfa_plots.loc[plot_index, 'plots']
        plot_zone = gfa_plots.loc[plot_index, 'zone']
        storey_height = storey_height_map.get(plot_zone, 5.0)

        cur_height_regs = height_regs.get_group(plot_id).copy() if plot_id in height_regs.groups else pd.DataFrame()
        cur_regs = reg_links_by_plot.get_group(
            plot_id).copy() if plot_id in reg_links_by_plot.groups else pd.DataFrame()

        cur_lha, cur_sbp = float('inf'), float('inf')
        parts = [gfa_plots.loc[plot_index, 'geometry']]
        part_storeys = [float('inf')]
        used_regs = []

        if not cur_height_regs.empty:
            reg_types = set(cur_height_regs['reg_type'].unique())
            if 'UrbanDesignGuideline' in reg_types:
                used_regs, part_storeys, parts = get_plot_parts(plot_index,
                                                                'UrbanDesignGuideline',
                                                                cur_height_regs,
                                                                gfa_plots)

            elif 'HeightControlPlan' in reg_types:
                non_null_abs_height = cur_height_regs['abs_height'].notna()
                cur_height_regs.loc[non_null_abs_height, 'storeys'] = (
                        cur_height_regs.loc[non_null_abs_height, 'abs_height'] // storey_height)
                used_regs, part_storeys, parts = get_plot_parts(plot_index,
                                                                'HeightControlPlan',
                                                                cur_height_regs,
                                                                gfa_plots)

        cur_regs = cur_regs[~cur_regs['reg'].isin(used_regs)]

        # Filter 'reg' values for each regulation type only once
        landed_regs = cur_regs.loc[cur_regs['reg_type'] == 'LandedHousingArea', 'reg']
        street_block_regs = cur_regs.loc[cur_regs['reg_type'] == 'StreetBlockPlan', 'reg']

        if not landed_regs.empty:
            cur_lha = lha.loc[lha['reg'].isin(landed_regs), 'storeys'].min()

        if not street_block_regs.empty:
            cur_sbp = sbp.loc[sbp['reg'].isin(street_block_regs), 'storeys'].min()

        cur_dcp = dcp[dcp['reg'].isin(cur_regs[cur_regs['reg_type'] == 'DevelopmentControlPlan']['reg'])]
        if plot_zone in residential_zones:
            allowed_res_programmes = set(gfa_plots.loc[plot_index, 'allowed_residential_types'] + ['Clinic'])
            cur_dcp = cur_dcp[cur_dcp['programme'].isin(allowed_res_programmes)]

        storey_limits = {}
        for programme in cur_dcp['programme'].unique():
            min_storeys = np.nanmin(
                np.concatenate(
                    [[float('inf'), cur_lha, cur_sbp], cur_dcp[cur_dcp['programme'] == programme]['storeys'].values]))
            storey_limits[programme] = [min(min_storeys, part_storey) for part_storey in part_storeys]

        combined_storeys.append(storey_limits)
        combined_parts.append(parts)

        sys.stdout.write(f"{count + 1}/{len(gfa_plots)} plots processed\r")
    sys.stdout.write(f"{len(gfa_plots)}/{len(gfa_plots)} plots processed\n")

    gfa_plots.loc[:, 'storeys'] = combined_storeys
    gfa_plots.insert(len(gfa_plots.columns), 'parts', combined_parts, allow_duplicates=False)

    return gfa_plots


def compute_part_gfa(allowed_storeys, footprint_areas, plot_area, site_coverage):
    """
    The function estimates allowed plot gfa by adding known footprint areas in plot parts for all allowed storeys.

    :param allowed_storeys: number of allowed storeys.
    :param footprint_areas: footprints allowed on a plot.
    :param plot_area: area of the whole plot.
    :param site_coverage: float value for allowed site coverage on a plot.
    :return: estimated plot part gfa. By default, plot has one part - whole plot.
    """
    part_gfa = 0

    for storey in range(len(footprint_areas)):
        if storey < allowed_storeys:
            storey_area = footprint_areas[storey]
            if (storey_area / plot_area) > site_coverage:
                storey_area = plot_area * site_coverage
            part_gfa += storey_area

    if allowed_storeys > len(footprint_areas):
        part_gfa += storey_area * (allowed_storeys - len(footprint_areas))

    return part_gfa


def compute_plot_gfa(gfa_plots, reg_links, sbp, dcp, residential_zones):
    """
    The function estimates gfa value for every allowed programme on a plot
    or a general gfa if there are no regulation exceptions for specific programmes.
    Gfa can only be estimated if plots have generated allowed footprints and derived storeys and plot parts
    or a known gpr value.

    :param gfa_plots: filtered plots GeoDataFrame for which gfa should be estimated.
    :param reg_links: a DataFrame containing plot ids, regulation ids, and regulation type.
    :param sbp: a GeoDataFrame containing StreetBlockPlan regulation content.
    :param dcp: a DataFrame containing DevelopmentControlPlan regulation content.
    :param residential_zones: list of residential zoning types.
    :return: estimated plot gfa for every allowed programme
    """

    # Pre-compute regulation mappings
    reg_links_by_plot = reg_links.groupby('plots')['reg'].apply(list).to_dict()
    sbp_by_reg = sbp.set_index('reg')
    dcp_by_reg = dcp.set_index('reg')

    all_gfas = []

    for count, plot in enumerate(gfa_plots.index):

        plot_id = gfa_plots.loc[plot, 'plots']
        plot_zone = gfa_plots.loc[plot, 'zone']
        plot_area = gfa_plots.loc[plot, 'geometry'].area
        plot_mp_gpr = gfa_plots.loc[plot, 'gpr']
        cur_parts = gfa_plots.loc[plot, 'parts']
        cur_footprints = gfa_plots.loc[plot, 'footprints']
        cur_storeys = gfa_plots.loc[plot, 'storeys']

        # Fetch current regulations
        cur_regs = reg_links_by_plot.get(plot_id, [])

        valid_sbp_regs = [reg for reg in cur_regs if reg in sbp_by_reg.index]
        valid_dcp_regs = [reg for reg in cur_regs if reg in dcp_by_reg.index]
        cur_sbp = sbp_by_reg.loc[valid_sbp_regs] if valid_sbp_regs else pd.DataFrame()
        cur_dcp = dcp_by_reg.loc[valid_dcp_regs] if valid_dcp_regs else pd.DataFrame()

        # Adjust for residential zones
        if plot_zone in residential_zones:
            allowed_res_programmes = set(gfa_plots.loc[plot, 'allowed_residential_types']) | {'Clinic'}
            cur_dcp = cur_dcp[cur_dcp['programme'].isin(allowed_res_programmes)]

        gfa = {}

        if cur_footprints and cur_storeys:
            # Pre-process footprints for all programmes
            processed_footprints = {
                programme: [
                    unary_union(footprint) if isinstance(footprint, MultiPolygon) else footprint
                    for footprint in footprints
                ]
                for programme, footprints in cur_footprints.items()
            }

            for programme, footprints in processed_footprints.items():

                programme_gfa = 0.0
                programme_storeys = cur_storeys[programme]

                # Gather all GPR values and compute the smallest
                gpr_list = [
                    *cur_dcp.loc[cur_dcp['programme'] == programme, 'gpr'].dropna(),
                    *cur_sbp.get('gpr', pd.Series(dtype=float)).dropna(),
                    plot_mp_gpr if pd.notna(plot_mp_gpr) else None
                ]
                cur_gpr = min(filter(lambda x: x is not None, gpr_list), default=None)

                if not np.any(np.isinf(programme_storeys)) and footprints:
                    site_coverage_list = cur_dcp[cur_dcp['programme'] == programme]['site_coverage']
                    cur_site_coverage = min(site_coverage_list, default=1.0)
                    footprint_areas = [footprint.area for footprint in footprints]

                    # Only one part -> no intersection with footprint(s) required.
                    if len(cur_parts) == 1:
                        # Compute GFA for a single part
                        programme_gfa = compute_part_gfa(
                            programme_storeys[0],
                            footprint_areas,
                            plot_area,
                            cur_site_coverage
                        )
                    else:
                        # Compute GFA for multiple parts
                        for part_index, part in enumerate(cur_parts):
                            part_programme_storeys = programme_storeys[part_index]
                            programme_gfa += compute_part_gfa(
                                part_programme_storeys,
                                [footprint.intersection(part).area for footprint in footprints],
                                plot_area,
                                cur_site_coverage
                            )
                else:
                    programme_gfa = np.nan

                # Check whether resultant gfa is within allowed gpr.
                if cur_gpr is not None:
                    if pd.isnull(programme_gfa):
                        programme_gfa = float('inf')
                    gfa[programme] = min(programme_gfa, plot_area * cur_gpr)
                else:
                    gfa[programme] = programme_gfa

        else:
            # Default programme (NaN) and no available storeys or footprints
            programme = np.nan
            gpr_list = [
                *cur_dcp['gpr'].dropna(),
                *cur_sbp.get('gpr', pd.Series(dtype=float)).dropna(),
                plot_mp_gpr if pd.notna(plot_mp_gpr) else None
            ]
            cur_gpr = min(filter(lambda x: x is not None, gpr_list), default=None)

            # Compute GFA based on GPR or assign NaN
            gfa[programme] = plot_area * cur_gpr if cur_gpr is not None else np.nan

        all_gfas.append(gfa)
        sys.stdout.write("{:d}/{:d} plots processed\r".format(count + 1, gfa_plots.shape[0]))
    sys.stdout.write("{:d}/{:d} plots processed\n".format(count + 1, gfa_plots.shape[0]))

    gfa_plots['gfa'] = all_gfas

    return gfa_plots






