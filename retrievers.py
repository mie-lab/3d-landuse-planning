import geopandas as gpd
import numpy as np
from utils import get_query_result, string_to_polygon
import logging
import pandas as pd

logger = logging.getLogger(__name__)

'''-------------------------------GET REGULATIONS----------------------------------'''


def get_development_control_plans(endpoint):
    """
    Queries the KG and returns development control plan (DCP) regulations.

    :param endpoint: SPARQL endpoint URL to query.
    :return: A DataFrame containing DevelopmentControlPlan regulations with attributes like 'gpr', 'storeys', 'setback', etc.
    """

    q = """
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    PREFIX oz: <http://www.theworldavatar.com/ontology/ontozoning/OntoZoning.owl#>
    PREFIX om: <http://www.ontology-of-units-of-measure.org/resource/om-2/>
    SELECT ?reg (SAMPLE(?gpr) AS ?gpr)
        (SAMPLE(?gpr_function) AS ?gpr_f)
        (SAMPLE(?setback_value) AS ?setback) 
        (SAMPLE(?storey) AS ?storeys)
        (SAMPLE(?storey_function) AS ?storey_f)
        (SAMPLE(?ftf_height) AS ?floor_height)
        (SAMPLE(?site_cov) AS ?site_coverage)
        (SAMPLE(?prog) AS ?programme) 
        (GROUP_CONCAT(DISTINCT(?road_category); separator=',') AS ?road_categories)
    WHERE { ?reg rdf:type opr:DevelopmentControlPlan ;
                opr:isConstrainedBy ?road_category . 
    OPTIONAL { ?reg opr:allowsGrossPlotRatio ?gpr_uri . 
               ?gpr_uri opr:hasValue ?gpr . 
           OPTIONAL { ?gpr_uri om:hasAggregateFunction ?gpr_func .
                    BIND(STRAFTER(STR(?gpr_func), '2/') AS ?gpr_function)
                    } 
    } 
    OPTIONAL {?reg opr:requiresSetback/om:hasValue/om:hasNumericValue ?setback_value 
    } 
    OPTIONAL {?reg opr:forProgramme ?programme_uri .
           BIND(STRAFTER(STR(?programme_uri), '#') AS ?prog)
           }    
    OPTIONAL { ?reg opr:allowsStoreyAggregate ?storey_aggr . 
               ?storey_aggr obs:numberOfStoreys ?storey . 
           OPTIONAL { ?storey_aggr om:hasAggregateFunction ?storey_func .
                     BIND(STRAFTER(STR(?storey_func), '2/') AS ?storey_function)
                     } 
    } 
    OPTIONAL {?reg opr:requiresFloorToFloorHeight/om:hasValue/om:hasNumericValue ?ftf_height 
    }  
    OPTIONAL { ?reg opr:allowsSiteCoverage ?site_cov_uri . 
           ?site_cov_uri opr:hasValue ?site_cov . } 
           }
    GROUP BY ?reg
    """

    qr = get_query_result(endpoint, q)
    numeric_columns = ['storeys', 'setback', 'gpr', 'site_coverage']
    for col in numeric_columns:
        qr[col] = pd.to_numeric(qr[col], errors='coerce')

    qr['road_categories'] = qr['road_categories'].apply(lambda x: x.split(',') if pd.notnull(x) else [])

    logger.info(f"{len(qr)} Development Control Plan items retrieved from the KG.")

    return qr


def get_street_block_plans(endpoint):
    """
    Queries the KG and retrieves street block plan (SBP) regulations.

    :param endpoint: SPARQL endpoint URL for querying SBP regulations.
    :return: A GeoDataFrame containing SBP regulation content with attributes like 'gpr', 'storeys', 'setback', 'setback_type', etc.
    """

    q = """
    PREFIX ocgml: <http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    PREFIX om: <http://www.ontology-of-units-of-measure.org/resource/om-2/>
    SELECT ?reg ?geom ?setback ?setback_type ?level ?storeys ?gpr
    WHERE { 
        GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobject/> {
            ?reg ocgml:id ?obj_id .
            BIND(IRI(REPLACE(STR(?reg), "cityobject", "genericcityobject")) AS ?gen_obj)
        }
        GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/surfacegeometry/> {
            ?s ocgml:cityObjectId ?gen_obj ;
               ocgml:GeometryType ?geom .
        }
        GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/planningregulations/> { 
            ?reg rdf:type opr:StreetBlockPlan .
            OPTIONAL {
                ?reg opr:requiresSetback | opr:requiresPartyWall ?setback_uri .
                ?setback_uri om:hasValue/om:hasNumericValue ?setback ;
                             rdf:type ?setback_type_uri ;
                             obs:atStorey/obs:atLevel ?level .
                BIND(STRAFTER(STR(?setback_type_uri), '#') AS ?setback_type)
            }
            OPTIONAL { ?reg opr:allowsStoreyAggregate/obs:numberOfStoreys ?storeys . }  
            OPTIONAL { ?reg opr:allowsGrossPlotRatio/opr:hasValue ?gpr . } 
        } }
    """

    qr = get_query_result(endpoint, q)
    geoms = gpd.GeoSeries(
        qr['geom'].map(lambda geo: string_to_polygon(geo, geodetic=True)),
        crs='EPSG:4326'
    ).to_crs(epsg=3857)

    sbp = gpd.GeoDataFrame(qr, geometry=geoms).drop(columns=['geom'])

    numeric_columns = ['storeys', 'setback', 'gpr', 'level']
    sbp[numeric_columns] = sbp[numeric_columns].apply(pd.to_numeric, errors='coerce')

    logger.info(f"{len(sbp)} StreetBlock Plan items retrieved from the KG.")

    return sbp


def get_height_control_plans(endpoint):
    """
    Queries the KG and retrieves height control plan (HCP) regulations.

    :param endpoint: SPARQL endpoint URL for querying the KG.
    :return: A GeoDataFrame containing HCP regulation content with attributes: 'abs_height', 'storeys', 'additional_type'.
    """

    q = """
    PREFIX ocgml: <http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    PREFIX om: <http://www.ontology-of-units-of-measure.org/resource/om-2/>
    SELECT ?reg ?geom ?abs_height ?height_f ?storeys ?storeys_f ?additional_type
    WHERE { 
        GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobject/> {
            ?reg ocgml:id ?obj_id .
            BIND(IRI(REPLACE(STR(?reg), "cityobject", "genericcityobject")) AS ?gen_obj)
        }
        GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/surfacegeometry/> {
            ?s ocgml:cityObjectId ?gen_obj ;
               ocgml:GeometryType ?geom .
        }
        GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/planningregulations/> { 
            ?reg rdf:type opr:HeightControlPlan .
            OPTIONAL {
                ?reg opr:allowsAbsoluteHeight ?height .
                ?height om:hasValue/om:hasNumericValue ?abs_height ;
                        om:hasAggregateFunction ?abs_height_f .
                BIND(STRAFTER(STR(?abs_height_f), "2/") AS ?height_f)
            }
            OPTIONAL {
                ?reg opr:allowsStoreyAggregate ?storey_aggr .
                ?storey_aggr om:hasAggregateFunction ?storey_aggr_f ; 
                              obs:numberOfStoreys ?storeys .
                BIND(STRAFTER(STR(?storey_aggr_f), "2/") AS ?storeys_f)
            }
            OPTIONAL {
                ?reg opr:hasAdditionalType ?detail_control .
                BIND(STRAFTER(STR(?detail_control), "#") AS ?additional_type)
            }
        } }
    """

    qr = get_query_result(endpoint, q)
    geoms = gpd.GeoSeries(
        qr['geom'].map(lambda geo: string_to_polygon(geo, geodetic=True)),
        crs='EPSG:4326'
    ).to_crs(epsg=3857)

    hcp = gpd.GeoDataFrame(qr, geometry=geoms).drop(columns=['geom'])
    hcp[['abs_height', 'storeys']] = hcp[['abs_height', 'storeys']].apply(pd.to_numeric, errors='coerce')

    logger.info(f"{len(hcp)} Height Control Plan items retrieved from the KG.")

    return hcp


def get_urban_design_guidelines(endpoint):
    """
    Queries the KG and retrieves urban design guidelines (UDG) regulations.

    :param endpoint: SPARQL endpoint URL for querying the KG.
    :return: A GeoDataFrame containing UDG regulation content with attributes: 'setback', 'storeys', 'partywall'.
    """

    q = """
    PREFIX ocgml: <http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    PREFIX om: <http://www.ontology-of-units-of-measure.org/resource/om-2/>
    SELECT ?reg 
           (SAMPLE(?storeys) AS ?storeys) 
           (SAMPLE(?storeys_f) AS ?storeys_f) 
           (SAMPLE(?setback) AS ?setback) 
           (SAMPLE(?setback_f) AS ?setback_f) 
           (SAMPLE(?partywall) AS ?partywall) 
           (SAMPLE(?additional_type) AS ?additional_type) 
           (SAMPLE(?geom) AS ?geom)
    WHERE {
        GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobject/> {
            ?reg ocgml:id ?obj_id .
            BIND(IRI(REPLACE(STR(?reg), "cityobject", "genericcityobject")) AS ?gen_obj)
        }
        GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/surfacegeometry/> {
            ?s ocgml:cityObjectId ?gen_obj ;
               ocgml:GeometryType ?geom .
        }
        GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/planningregulations/> {
            ?reg rdf:type opr:UrbanDesignGuideline .

            OPTIONAL {
                ?reg opr:allowsStoreyAggregate ?storey_aggr .
                ?storey_aggr om:hasAggregateFunction ?storey_aggr_f ; 
                              obs:numberOfStoreys ?storeys .
                BIND(STRAFTER(STR(?storey_aggr_f), "2/") AS ?storeys_f)
            }
            OPTIONAL {
                ?reg opr:requiresSetback ?setback_uri .
                ?setback_uri om:hasValue/om:hasNumericValue ?setback ;
                             om:hasAggregateFunction ?setback_f_uri .
                BIND(STRAFTER(STR(?setback_f_uri), "2/") AS ?setback_f)
            }
            OPTIONAL {
                ?reg opr:requiresPartyWall ?partywall_uri .
                BIND(BOUND(?partywall_uri) AS ?partywall)
            }
            OPTIONAL {
                ?reg opr:hasAdditionalType ?detail_control .
                BIND(STRAFTER(STR(?detail_control), "#") AS ?additional_type)
            } 
        } 
    }
    GROUP BY ?reg
    """

    qr = get_query_result(endpoint, q)

    geoms = gpd.GeoSeries(
        qr['geom'].map(lambda geo: string_to_polygon(geo, geodetic=True)),
        crs='EPSG:4326'
    ).to_crs(epsg=3857)

    udg = gpd.GeoDataFrame(qr, geometry=geoms).drop(columns=['geom'])
    udg[['storeys', 'setback']] = udg[['storeys', 'setback']].apply(pd.to_numeric, errors='coerce')

    logger.info(f"{len(udg)} Urban Design Guideline items retrieved from the KG.")

    return udg


def get_landed_housing_areas(endpoint):
    """
    Queries the KG and retrieves landed housing area (LHA) regulations.

    :param endpoint: SPARQL endpoint URL to query.
    :return: A DataFrame containing LandedHousingArea regulation content: 'storeys'.
    """
    q = """
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    SELECT ?reg ?storeys
    WHERE {
        VALUES ?type {opr:LandedHousingArea opr:GoodClassBungalowArea}
        ?reg rdf:type ?type ;
             opr:allowsStoreyAggregate/obs:numberOfStoreys ?storeys .
    }
    """

    qr = get_query_result(endpoint, q)
    qr['storeys'] = pd.to_numeric(qr['storeys'], errors='coerce')

    logger.info(f"{len(qr)} Landed Housing Area items retrieved from the KG.")

    return qr


def get_type_regulations(endpoint):
    """
    Retrieves type-based planning regulations from the specified SPARQL endpoint.

    :param endpoint: The SPARQL endpoint URL for querying type-based planning regulations.
    :return: A DataFrame containing type-based planning regulations.
    """

    q = """
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    PREFIX oz: <http://www.theworldavatar.com/ontology/ontozoning/OntoZoning.owl#>
    PREFIX om: <http://www.ontology-of-units-of-measure.org/resource/om-2/>

    SELECT ?reg 
       (GROUP_CONCAT(DISTINCT ?zone; separator=",") AS ?for_zones)
       (GROUP_CONCAT(DISTINCT ?neighbour_zone; separator=",") AS ?neighbour_zones)
       (GROUP_CONCAT(DISTINCT ?programme; separator=",") AS ?for_programme)
       (GROUP_CONCAT(DISTINCT ?areas; separator=",") AS ?in_area_regs)
       (SAMPLE(?gpr_value) AS ?requires_gpr)
       (SAMPLE(?function) AS ?gpr_function)
       (SAMPLE(?fringe) AS ?for_fringe_plot)
       (SAMPLE(?corner) AS ?for_corner_plot)
       (SAMPLE(?gcba) AS ?abuts_gcba)
       (SAMPLE(?gcba_in) AS ?in_gcba)
       (SAMPLE(?central_area) AS ?in_central_area)
       (SAMPLE(?road) AS ?abuts_road)
    WHERE { 
    GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/planningregulations/> { 
        ?reg rdf:type opr:DevelopmentControlPlan .

        OPTIONAL { 
            ?reg opr:forZoningType ?zone_type_uri .
            BIND(STRAFTER(STR(?zone_type_uri), '#') AS ?zone) 
        }
        OPTIONAL { 
            ?reg opr:forNeighbourZoneType ?neighbour_zone_uri .
            BIND(STRAFTER(STR(?neighbour_zone_uri), '#') AS ?neighbour_zone) 
        }

        OPTIONAL { ?reg opr:forPlotContainedIn ?areas . }

        OPTIONAL { 
        ?reg opr:forProgramme ?programme_uri .
        BIND(STRAFTER(STR(?programme_uri), '#') AS ?programme) 
        }

        OPTIONAL { 
        ?reg opr:allowsGrossPlotRatio ?gpr .
        ?gpr opr:hasValue ?gpr_value .
        OPTIONAL { ?gpr om:hasAggregateFunction ?function . } 
        }

        OPTIONAL { ?reg opr:plotAbuts1-3RoadCategory ?road . }
        OPTIONAL { ?reg opr:plotAbutsGoodClassBungalowArea ?gcba . }
        OPTIONAL { ?reg opr:plotInGoodClassBungalowArea ?gcba_in . }
        OPTIONAL { ?reg opr:plotInCentralArea ?central_area . }
        OPTIONAL { ?reg opr:forFringePlot ?fringe . }
        OPTIONAL { ?reg opr:forCornerPlot ?corner . }
    } }
    GROUP BY ?reg
    """

    type_regs = get_query_result(endpoint, q)
    type_regs.replace('', np.nan, inplace=True)

    type_regs['for_fringe_plot'] = type_regs['for_fringe_plot'].notna()
    type_regs['for_corner_plot'] = type_regs['for_corner_plot'].notna()
    type_regs['abuts_road'] = type_regs['abuts_road'].notna()

    for column in ['for_zones', 'neighbour_zones', 'for_programme', 'in_area_regs']:
        type_regs[column] = type_regs[column].apply(lambda x: x.split(',') if pd.notna(x) else [])

    numeric_fields = ['requires_gpr', 'abuts_gcba', 'in_gcba']
    type_regs[numeric_fields] = type_regs[numeric_fields].apply(pd.to_numeric, errors='coerce')

    logger.info(f'{len(type_regs)} type regulations retrieved form the KG.')

    return type_regs


def get_urban_design_areas(endpoint):
    """
    Retrieves Urban Design Areas from the KG. The results are used in `instantiate_urban_design_guidelines()`.

    :param endpoint: SPARQL endpoint URL to query urban design areas data.
    :return: A dictionary mapping Urban Design Area names to their URIs.
    """

    q = """
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    SELECT ?uda ?name     
    WHERE { GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/planningregulations/>
    {
    ?uda  rdf:type opr:UrbanDesignArea ;
    opr:hasName ?name .
    } } 
    """

    uda = get_query_result(endpoint, q)
    area_dict = {}

    for i in uda.index:
        area_dict[uda.loc[i, 'name']] = uda.loc[i, 'uda']

    logger.info(f"{len(uda)} Urban Design Area items retrieved from the KG.")

    return area_dict


def get_planning_boundaries(endpoint):
    """
    Retrieves planning boundary URIs and their corresponding names from the KG.

    :param endpoint: SPARQL endpoint URL to query planning boundary data.
    :return: A dictionary mapping planning boundary names to their URIs.
    """

    q = """
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    SELECT ?pa ?name
    WHERE { ?pa rdf:type opr:PlanningBoundary ;
                opr:hasName ?name . } 
                """

    pb = get_query_result(endpoint, q)

    logger.info(f"{len(pb)} Planning Boundary items retrieved from the KG.")

    return pb


def get_road_categories(endpoint):
    """
    Queries the KG and retrieves road category regulations.

    :param endpoint: SPARQL endpoint URL to query.
    :return: A DataFrame containing road category regulation content: 'category' and 'buffer'.
    """
    q = """
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    PREFIX om: <http://www.ontology-of-units-of-measure.org/resource/om-2/>
    SELECT ?road_reg 
           (SAMPLE(STRAFTER(STR(?type), '#RoadCategory')) AS ?category)
           (SAMPLE(?road_buffer) AS ?buffer)
    WHERE { 
        ?reg opr:isConstrainedBy ?road_reg .
        ?road_reg rdf:type ?type ;
                  opr:requiresRoadBuffer/om:hasValue/om:hasNumericValue ?road_buffer .
    }
    GROUP BY ?road_reg
    """

    road_cat = get_query_result(endpoint, q)
    road_cat['buffer'] = pd.to_numeric(road_cat['buffer'], errors='coerce')
    road_cat['category'] = pd.to_numeric(road_cat['category'], errors='coerce')

    logger.info(f"{len(road_cat)} Road Category items retrieved from the KG.")

    return road_cat


def get_regulation_overlaps(endpoint, plots, accuracy, boolean):
    """
    Computes overlaps between area-based planning regulations and plots.

    :param endpoint: The SPARQL endpoint URL to query regulation geometries.
    :param plots: A GeoDataFrame containing plot geometries and associated data.
    :param accuracy: The minimum proportion of the overlap area to consider a match.
    :param boolean: Whether to select the largest overlap for each plot regardless of accuracy.
    :return: A GeoDataFrame containing the filtered intersections between regulations and plots.
    """

    q = """
    PREFIX ocgml:<http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
    SELECT ?obj_id ?geom
    WHERE { 
    GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/surfacegeometry/> 
    {
    ?s ocgml:cityObjectId ?obj_id ;
    ocgml:GeometryType ?geom . } } 
    """

    qr = get_query_result(endpoint, q)
    geoms = gpd.GeoSeries(
        qr['geom'].map(lambda geo: string_to_polygon(geo, geodetic=True)),
        crs='EPSG:4326'
    ).to_crs(epsg=3857)

    area_regs = gpd.GeoDataFrame(qr, geometry=geoms).drop(columns=['geom'])

    intersection = gpd.overlay(area_regs, plots, how='intersection', keep_geom_type=True)
    intersection['int_area'] = intersection.area

    if boolean:
        regulation_overlaps = (
            intersection.sort_values(['plots', 'int_area'], ascending=False).drop_duplicates(subset=['plots']))
    else:
        regulation_overlaps = intersection.loc[lambda df: (df['int_area'] / df['plot_area']) > accuracy]

    logger.info(f"{len(regulation_overlaps)} Overlaps estimated.")

    return regulation_overlaps


'''-------------------------------------GET PLOT INFORMATION-----------------------------------'''


def get_plots(endpoint):
    """
    The function queries the KG and retrieves Singapore's Masterplan 2019 plot data.
    :param endpoint: KG endpoint url to which method query is sent.
    :return: plots GeoDataFrame
    """

    q = """
    PREFIX ocgml:<http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
    SELECT ?plots ?geom ?zone ?gpr
    WHERE { GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobject/>
    { 
    ?plots ocgml:id ?obj_id .
    BIND(IRI(REPLACE(STR(?plots), "cityobject", "genericcityobject")) AS ?gen_obj) }

    GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/surfacegeometry/> {
    ?s ocgml:cityObjectId ?gen_obj ;
    ocgml:GeometryType ?geom . 
    hint:Prior hint:runLast "true" . }

    GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobjectgenericattrib/> {
    ?attr ocgml:cityObjectId ?plots ;
    ocgml:attrName 'LU_DESC' ;
    ocgml:strVal ?zone . 

    ?attr1 ocgml:cityObjectId ?plots ;
    ocgml:attrName 'GPR' ;
    ocgml:strVal ?gpr . } }
    """

    qr = get_query_result(endpoint, q)
    geoms = gpd.GeoSeries(
        qr['geom'].map(lambda geo: string_to_polygon(geo, geodetic=True)),
        crs='EPSG:4326'
    ).to_crs(epsg=3857)

    plots = gpd.GeoDataFrame(qr, geometry=geoms).drop(columns=['geom'])
    plots.geometry = plots.geometry.simplify(0.1)

    plots['plot_area'] = plots.area
    plots['gpr'] = pd.to_numeric(plots['gpr'], errors='coerce')

    logger.info(f"{len(plots)} Plots retrieved from the KG.")

    return plots


def get_neighbours(plots, endpoint):
    """
    The method queries the KG and retrieves plot neighbour ids and appends to plot GeoDataFrame.

    :param plots: Singapore's Masterplan 2019 plots GeoDataFrame queried from the KG.
    :param endpoint: KG endpoint url to which method query is sent.
    :return: a modified plots GeoDataFrame with 'neighbour' column containing neighbour id list.
    """

    q = """
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    SELECT ?plots ?neighbour
    WHERE { ?plots obs:hasNeighbour ?neighbour . } 
    """

    plot_neighbours = get_query_result(endpoint, q)
    plot_neighbours = plot_neighbours.groupby(by='plots')['neighbour'].apply(set)
    plots = plots.merge(plot_neighbours, left_on='plots', right_index=True, how='left')

    logger.info("Neighbors retrieved from the KG and set.")

    return plots


def get_plot_neighbour_types(plots, endpoint):
    """
    Queries and enriches a DataFrame with residential plot neighbor types.

    :param plots: A DataFrame containing plot data to be enriched.
    :param endpoint: SPARQL endpoint URL for querying neighbor information.
    :return: A DataFrame with additional neighbor attributes.
    """

    q = """
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    PREFIX oz: <http://www.theworldavatar.com/ontology/ontozoning/OntoZoning.owl#>
    SELECT ?plots 
           (GROUP_CONCAT(?type; separator=",") AS ?neighbour_road_type) 
           (GROUP_CONCAT(DISTINCT ?zone; separator=",") AS ?neighbour_zones) 
           (IF(COUNT(?gcba) > 0, 1, 0) AS ?abuts_gcba)
           (IF(COUNT(?reg) > 0, 1, 0) AS ?in_central_area)
    WHERE { 
        ?plots obs:hasNeighbour ?neighbour .
        OPTIONAL {
            ?neighbour oz:hasZone ?zone_uri .
            BIND(STRAFTER(STR(?zone_uri), '#') AS ?zone)
        }
        OPTIONAL {
            ?neighbour obs:hasRoadType ?type .
        }
        OPTIONAL {
            ?gcba opr:appliesTo ?neighbour ;
                rdf:type opr:GoodClassBungalowArea .
        }
        OPTIONAL {
            ?reg opr:appliesTo ?plots ;
                rdf:type opr:CentralArea .
        }
    } 
    GROUP BY ?plots
    """

    neighbours = get_query_result(endpoint, q)
    plots = plots.merge(neighbours, how='left', on='plots')

    for col in ['neighbour_road_type', 'neighbour_zones']:
        plots[col] = (
            plots[col]
            .fillna('')
            .str.split(',')
            .apply(lambda lst: [] if lst == [''] else lst)
        )

    num_cols = ['abuts_gcba', 'in_central_area']
    plots[num_cols] = (
        plots[num_cols]
        .apply(pd.to_numeric, errors='coerce')
        .fillna(0)
        .astype(int)
    )

    logger.info("Neighbors types retrieved from the KG and set.")

    return plots


def get_plot_allowed_programmes(plots, endpoint):
    """
    Queries allowed development programmes and regulations applicable to residential plots.

    :param plots: A DataFrame containing residential plot information, including their identifiers.
    :param endpoint: A string representing the SPARQL endpoint URL for querying regulations.
    :return: A DataFrame enriched with additional information on applicable programmes and regulations.
    """
    q = """
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    PREFIX oz: <http://www.theworldavatar.com/ontology/ontozoning/OntoZoning.owl#>
    SELECT ?plots 
           (GROUP_CONCAT(DISTINCT ?pb; separator=',') AS ?in_pb) 
           (GROUP_CONCAT(DISTINCT ?sbp_programme_name; separator=",") AS ?sbp_programmes) 
           (GROUP_CONCAT(DISTINCT ?lha; separator=',') AS ?in_lha) 
           (GROUP_CONCAT(DISTINCT ?lha_programme_name; separator=",") AS ?lha_programmes) 
           (IF(COUNT(?gcba) > 0, 1, 0) AS ?in_gcba)
    WHERE {
        GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/ontozone/> {
            ?plots oz:hasZone ?zone .
        }
        OPTIONAL {
            ?sbp rdf:type opr:StreetBlockPlan ;
                 opr:appliesTo ?plots ;
                 oz:allowsProgramme ?sbp_programme .
            BIND(STRAFTER(STR(?sbp_programme), '#') AS ?sbp_programme_name)
        }
        OPTIONAL {
            ?lha rdf:type opr:LandedHousingArea ;
                 opr:appliesTo ?plots ;
                 oz:allowsProgramme ?lha_programme .
            BIND(STRAFTER(STR(?lha_programme), '#') AS ?lha_programme_name)
        }
        OPTIONAL {
            ?gcba rdf:type opr:GoodClassBungalowArea ;
                  opr:appliesTo ?plots .
        }
        OPTIONAL {
            ?pb opr:appliesTo ?plots ;
                rdf:type opr:PlanningBoundary .
        }
    }
    GROUP BY ?plots
    """

    applicable_regulations = get_query_result(endpoint, q)
    plots = plots.merge(applicable_regulations, how='left', on='plots')

    for col in ['sbp_programmes', 'lha_programmes', 'in_pb', 'in_lha']:
        plots[col] = plots[col].apply(lambda x: x.split(',') if (pd.notna(x) and x) else [])

    plots['in_gcba'] = pd.to_numeric(plots['in_gcba'].fillna(0), errors='coerce')

    logger.info("Allowed programmes derived and set.")

    return plots


def get_plot_properties(plots, endpoint):
    """
    Queries plot properties and enriches the provided DataFrame with additional attributes.

    :param plots: A DataFrame containing the initial plot information.
    :param endpoint: The SPARQL endpoint URL for querying plot properties.
    :return: An enriched DataFrame with additional plot properties such as average width, average depth, zone,
             corner plot status, and fringe plot status.
    """

    q = """
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    PREFIX om: <http://www.ontology-of-units-of-measure.org/resource/om-2/>
    SELECT ?plots {name}
    WHERE {{ GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/buildablespace/> 
    {{
    OPTIONAL {{ ?plots obs:{predicate}/om:hasValue/om:hasNumericValue {name} . }}
     }} 
    }}
    """

    width_properties = get_query_result(endpoint, q.format(predicate='hasWidth', name='?avg_width'))
    plots = plots.merge(width_properties, how='left', on='plots')
    plots['avg_width'] = pd.to_numeric(plots['avg_width'], errors='coerce')

    depth_properties = get_query_result(endpoint, q.format(predicate='hasDepth', name='?avg_depth'))
    plots = plots.merge(depth_properties, how='left', on='plots')
    plots['avg_depth'] = pd.to_numeric(plots['avg_depth'], errors='coerce')

    q = """
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    PREFIX om: <http://www.ontology-of-units-of-measure.org/resource/om-2/>
    SELECT ?plots ?corner_plot ?fringe_plot
    WHERE { GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/buildablespace/> {
        OPTIONAL { 
        ?plots obs:isCornerPlot ?corner_plot . 
        }
        OPTIONAL { 
        ?plots obs:atResidentialFringe ?fringe_plot . 
        }
    } }
    """

    qr = get_query_result(endpoint, q)
    plots = plots.merge(qr, how='left', on='plots')

    q = """
        PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
        PREFIX om: <http://www.ontology-of-units-of-measure.org/resource/om-2/>
        SELECT ?plots ?road_type
        WHERE { GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/buildablespace/> {
            ?plots obs:hasRoadType ?road_type . 
        } }
        """

    qr = get_query_result(endpoint, q)
    plots = plots.merge(qr, how='left', on='plots')

    q = """
    PREFIX oz: <http://www.theworldavatar.com/ontology/ontozoning/OntoZoning.owl#>
    SELECT ?plots ?zone
    WHERE { GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/ontozone/> {
        ?plots oz:hasZone ?zone_uri . 
        BIND(STRAFTER(STR(?zone_uri), '#') AS ?zone)
    } }
    """
    qr = get_query_result(endpoint, q)
    plots = plots.merge(qr, how='left', on='plots')

    logger.info("Plot properties retrieved from the KG and set.")

    return plots


def get_regulation_links(endpoint):
    """
    The function queries the KG and retrieves planning regulation and plot links.

    :param endpoint: KG endpoint url to which method query is sent.
    :return: a DataFrame containing plot ids, regulation ids, and regulation type.
    """
    q = """
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    SELECT ?plots ?reg ?reg_type
    WHERE { GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/planningregulations/> {
    ?reg opr:appliesTo ?plots ;
         rdf:type ?type . }
    BIND(STRAFTER(STR(?type), '#') AS ?reg_type) } 
    """
    qr = get_query_result(endpoint, q)

    logger.info(f"{len(qr)} Regulation links retrieved from the KG.")

    return qr


'''----------------------------------GET ANALYSIS RESULTS---------------------------------'''


def get_regulation_counts(endpoint, reg_name):
    """
    Retrieves the count of distinct regulations of a given type from the specified SPARQL endpoint.
    The results correspond to Table 2 in 3D Land Use Planning paper.

    :param endpoint: The SPARQL endpoint URL to query.
    :param reg_name: The name of the regulation type to count (e.g., "HeightControlPlan").
    :return: The count of distinct regulations of the specified type as an integer.
    """

    q = f"""
        PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT (COUNT(DISTINCT ?reg) AS ?reg_count)
        WHERE {{ 
            GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/planningregulations/> 
            {{ ?reg rdf:type opr:{reg_name} . }} 
        }}
    """

    qr = get_query_result(endpoint, q)

    logger.info("Regulation counts retrieved from the KG.")

    return int(qr.loc[0, 'reg_count'])


def get_regulation_plot_counts(endpoint, reg_name):
    """
    Queries the Knowledge Graph (KG) to retrieve planning regulation and plot links.

    :param endpoint: The SPARQL endpoint URL for querying regulations and plots.
    :param reg_name: The name of the regulation type to filter (e.g., "HeightControlPlan").
    :return: A DataFrame containing plot identifiers (`plots`) linked to the specified regulation type.
    """
    q = f"""
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    SELECT DISTINCT ?plots
    WHERE {{ GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/planningregulations/> 
    {{ ?reg rdf:type opr:{reg_name} . 
       ?reg opr:appliesTo ?plots . }}
            }} 
    """

    qr = get_query_result(endpoint, q)

    logger.info("Plot counts per regulations retrieved from the KG.")

    return qr


def get_plot_ids(endpoint, reg_id):
    """
    Queries the Knowledge Graph (KG) and retrieves plot identifiers linked to a specific regulation.

    :param endpoint: The SPARQL endpoint URL to query.
    :param reg_id: The identifier of the regulation for which plots are to be retrieved.
    :return: A list of plot identifiers (strings) linked to the specified regulation.
    """
    q = f"""
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    SELECT ?plot
    WHERE {{
        GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/planningregulations/> {{
            <{reg_id}> opr:appliesTo ?plot . }}
    }}
    """

    qr = get_query_result(endpoint, q)

    if not qr.empty:
        qr_list = qr['plot'].tolist()
    else:
        qr_list = []
    logger.info("Plots linked to a specific regulation retrieved from the KG.")

    return qr_list


def get_frequent_regulation_instances(endpoint):
    """
    Retrieves frequent regulations from a SPARQL endpoint, aggregating associated programmes, zones, and plot counts.

    :param endpoint: The SPARQL endpoint URL to query for regulation details.
    :return: A DataFrame containing regulation instances.
    """

    q = """
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulations/OntoPlanningRegulations.owl#>
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    PREFIX oz: <http://www.theworldavatar.com/ontology/ontozoning/OntoZoning.owl#>
    PREFIX om: <http://www.ontology-of-units-of-measure.org/resource/om-2/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT ?reg 
        ?type
        (SAMPLE(?programme_name) AS ?programmes)
        (GROUP_CONCAT(DISTINCT ?zone_name; separator=",") AS ?zones) 
        (COUNT(DISTINCT ?plot) AS ?plots)
    WHERE { 
    GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/planningregulations/> { 
        ?reg rdf:type ?regType .
        VALUES ?regType {
        opr:HeightControlPlan
        opr:DevelopmentControlPlan
        opr:ConservationArea
        opr:CentralArea
        opr:Monument
        opr:PlanningBoundary
        opr:StreetBlockPlan
        opr:UrbanDesignArea
        opr:UrbanDesignGuideline
        opr:LandedHousingArea
        }
        BIND(STRAFTER(STR(?regType), "#") AS ?type)
        OPTIONAL { 
        ?reg opr:appliesTo ?plot .
        }
        OPTIONAL {
        ?reg opr:forProgramme ?programme .
        BIND(STRAFTER(STR(?programme), '#') AS ?programme_name)
        }
        OPTIONAL { 
        ?reg opr:forZoningType ?zone .
        BIND(STRAFTER(STR(?zone), '#') AS ?zone_name)
        }
        OPTIONAL { 
        ?reg opr:forPlotsInGooClassBungalowArea ?in_gcba .
        } 
    }
    }
    GROUP BY ?reg ?type ?programme_name
    ORDER BY DESC(?plots)
    """

    qr = get_query_result(endpoint, q)

    logger.info("Frequent regulations retrieved from the KG.")

    return qr


def get_gfas(endpoint):
    """
    Queries a SPARQL endpoint to retrieve planning regulation and plot links.

    :param endpoint: The URL of the SPARQL endpoint to query.
    :return: A pandas DataFrame containing plot IDs, regulation IDs, and regulation types.
    """

    q = """
    PREFIX obs: <http://www.theworldavatar.com/ontology/ontobuildablespace/OntoBuildableSpace.owl#>
    PREFIX om: <http://www.ontology-of-units-of-measure.org/resource/om-2/>
    SELECT ?plots ?gfa_value ?case
    WHERE {
    GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/buildablespace/> {
    ?plots obs:hasBuildableSpace ?bspace.

    OPTIONAL{?bspace obs:hasAllowedGFA ?gfa.
    ?gfa om:hasValue/om:hasNumericValue ?gfa_value.
    }

    OPTIONAL{?bspace obs:forZoningCase ?uri_case
    BIND(STRAFTER(STR(?uri_case), '#') AS ?case)
    }

    } }
    ORDER BY ?plots
    """
    qr = get_query_result(endpoint, q)
    qr['gfa_value'] = pd.to_numeric(qr['gfa_value'], errors='coerce')

    return qr
