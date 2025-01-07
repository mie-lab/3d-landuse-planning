import pandas as pd
from GFAOntoManager import *
from triple_dataset import TripleDataset
from utils import get_query_result, get_neighbor_links
from retrievers import get_plots, get_regulation_overlaps
import logging

logger = logging.getLogger(__name__)

'''--------------------------------INSTANTIATE REGULATIONS----------------------------------'''


def instantiate_height_control(endpoint, out_dir, out_endpoint=None):
    """
    Query height control city object IDs and instantiate height control regulation content.

    :param endpoint: SPARQL endpoint to query height control data.
    :param out_dir: Directory to write output files.
    :param out_endpoint: SPARQL endpoint to upload the generated triples.
    """

    q = """PREFIX ocgml:<http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
    SELECT ?city_obj ?ext_ref ?unit_type ?height
    WHERE {GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobjectgenericattrib/> 
    {
    ?attr_1 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'ExtRef' ;
        ocgml:uriVal ?ext_ref .

    ?attr_2 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'HT_CTL_TYP' ;
        ocgml:strVal ?unit_type .

    ?attr_3 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'HT_CTL_TXT' ;
        ocgml:strVal ?height . } 
    } 
    """

    qr = get_query_result(endpoint, q)

    hcp = TripleDataset(out_dir)
    for i in qr.index:
        city_obj = qr.loc[i, 'city_obj']
        ext_ref = qr.loc[i, 'ext_ref']
        height = qr.loc[i, 'height']
        unit_type = qr.loc[i, 'unit_type']
        hcp.create_height_control_triples(city_obj, str(ext_ref), height, unit_type)

    hcp.write_triples("height_control_triples", out_endpoint)

    logger.info(f"{len(qr)} height control items have been instantiated.")


def instantiate_conservation_areas(endpoint, out_dir, out_endpoint=None):
    """
    Query city object IDs for conservation areas and instantiate related regulation content.

    :param endpoint: SPARQL endpoint to query conservation areas data.
    :param out_dir: Directory to write output files.
    :param out_endpoint: SPARQL endpoint to upload the generated triples.
    """

    query = """PREFIX ocgml: <http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
    SELECT ?city_obj ?ext_ref
    WHERE {
    GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobjectgenericattrib/> {

    ?genAttr ocgml:cityObjectId ?city_obj .
    ?genAttr ocgml:attrName 'ExtRef' ;
    ocgml:uriVal ?ext_ref . 
    } } """

    con = get_query_result(endpoint, query)
    con_dataset = TripleDataset(out_dir)
    for i in con.index:
        con_dataset.create_conservation_triples(con.loc[i, 'city_obj'],
                                                con.loc[i, 'ext_ref'])
    con_dataset.write_triples("conservation_areas_triples", out_endpoint)

    logger.info(f"{len(con)} conservation area items have been instantiated.")


def instantiate_central_area(endpoint, out_dir, out_endpoint=None):
    """
    Query city object IDs for the central area and instantiate related regulation content.

    :param endpoint: SPARQL endpoint to query central area data.
    :param out_dir: Directory to write output files.
    :param out_endpoint: SPARQL endpoint to upload the generated triples.
    """

    q = """PREFIX ocgml: <http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
        SELECT ?city_obj ?ext_ref
        WHERE {
        GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobjectgenericattrib/> 
        {
        ?genAttr ocgml:cityObjectId ?city_obj .
        ?genAttr ocgml:attrName 'ExtRef' ;
        ocgml:uriVal ?ext_ref . 
        } } """

    ca = get_query_result(endpoint, q)
    ca_dataset = TripleDataset(out_dir)
    for i in ca.index:
        ca_dataset.create_central_area_triples(ca.loc[i, 'city_obj'],
                                               ca.loc[i, 'ext_ref'])
    ca_dataset.write_triples("central_area_triples", out_endpoint)

    logger.info(f"{len(ca)} central area items have been instantiated.")


def instantiate_planning_boundaries(endpoint, out_dir, out_endpoint=None):
    """
    Query city object IDs for Planning Boundaries and instantiate related regulation content.

    :param endpoint: SPARQL endpoint to query planning boundaries data.
    :param out_dir: Directory to write the output N-Quads file.
    :param out_endpoint: SPARQL endpoint to upload the generated triples.
    """

    q = """
    PREFIX ocgml: <http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
    SELECT ?city_obj ?ext_ref ?planning_area ?region
    WHERE {GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobjectgenericattrib/> 
    {
    ?genAttr ocgml:cityObjectId ?city_obj ;
    ocgml:attrName 'ExtRef' ;
    ocgml:uriVal ?ext_ref .

    ?genAttr2 ocgml:cityObjectId ?city_obj ;
    ocgml:attrName 'PLN_AREA_N';
    ocgml:strVal ?planning_area .

    ?genAttr3 ocgml:cityObjectId ?city_obj ;
    ocgml:attrName 'REGION_N';
    ocgml:strVal ?region . 
    } } """

    qr = get_query_result(endpoint, q)
    pb = TripleDataset(out_dir)
    for i in qr.index:
        pb.create_planning_boundaries_triples(qr.loc[i, 'city_obj'],
                                              qr.loc[i, 'ext_ref'],
                                              qr.loc[i, 'planning_area'],
                                              qr.loc[i, 'region'])
    pb.write_triples("planning_areas_triples", out_endpoint)

    logger.info(f"{len(qr)} planning boundary items have been instantiated.")


def instantiate_monuments(endpoint, out_dir, out_endpoint=None):
    """
    Query city object IDs for Monuments and instantiate related regulation content.

    :param endpoint: SPARQL endpoint to query monuments data.
    :param out_dir: Directory to write the output N-Quads file.
    :param out_endpoint: SPARQL endpoint to upload the generated triples.
    """

    q = """
    PREFIX ocgml: <http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
    SELECT ?city_obj ?ext_ref ?name
    WHERE {
    GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobjectgenericattrib/> 
    {
    ?genAttr ocgml:cityObjectId ?city_obj ;
    ocgml:attrName 'ExtRef' ;
    ocgml:uriVal ?ext_ref .

    ?genAttr2 ocgml:cityObjectId ?city_obj ;
    ocgml:attrName 'NAME' ; 
    ocgml:strVal ?name . 
    } } 
    """

    qr = get_query_result(endpoint, q)
    monument = TripleDataset(out_dir)
    for i in qr.index:
        monument.create_monument_triples(qr.loc[i, 'city_obj'], qr.loc[i, 'ext_ref'], qr.loc[i, 'name'])
    monument.write_triples("monuments_triples", out_endpoint)

    logger.info(f"{len(qr)} monument items have been instantiated.")


def instantiate_landed_housing_areas(endpoint, out_dir, out_endpoint=None):
    """
    Query and instantiate Landed Housing Area regulation content.

    :param endpoint: SPARQL endpoint to query landed housing areas data.
    :param out_dir: Directory to write the output N-Quads file.
    :param out_endpoint: SPARQL endpoint to upload the generated triples.
    """

    q = """
    PREFIX ocgml: <http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
    SELECT ?city_obj ?ext_ref ?height ?type ?area
    WHERE {GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobjectgenericattrib/> 
    {
    ?genAttr1 ocgml:cityObjectId ?city_obj ;
    ocgml:attrName 'STY_HT' ;
    ocgml:strVal ?height .

    ?genAttr2 ocgml:cityObjectId ?city_obj ;
    ocgml:attrName 'TYPE' ;
    ocgml:strVal ?type. 

    ?genAttr3 ocgml:cityObjectId ?city_obj ;
    ocgml:attrName 'CLASSIFCTN' ;
    ocgml:strVal ?area.

    ?genAttr4 ocgml:cityObjectId ?city_obj ;
    ocgml:attrName 'ExtRef' ;
    ocgml:uriVal ?ext_ref . } 
    } 
    """

    qr = get_query_result(endpoint, q)
    lha = TripleDataset(out_dir)
    for i in qr.index:
        lha.create_landed_housing_areas_triples(qr.loc[i, 'city_obj'],
                                                str(qr.loc[i, 'ext_ref']),
                                                qr.loc[i, 'height'],
                                                qr.loc[i, 'type'],
                                                qr.loc[i, 'area'])
    lha.write_triples("landed_housing_areas_triples", out_endpoint)

    logger.info(f"{len(qr)} Landed housing items have been instantiated.")


def instantiate_street_block_plan(endpoint, out_dir, out_endpoint=None):
    """
    Query and instantiate Street Block Plan regulation content.

    :param endpoint: SPARQL endpoint to query street block plans data.
    :param out_dir: Directory to write the output N-Quads file.
    :param out_endpoint: SPARQL endpoint to upload the generated triples.
    """

    q = """
    PREFIX ocgml: <http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
    SELECT ?city_obj ?front_setback ?side_setback ?rear_setback ?partywall_setback ?storeys ?gpr ?ext_ref 
    ?allowed_programmes ?landuse ?name
    WHERE {GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobjectgenericattrib/> 
    { 
    ?attr_0 ocgml:cityObjectId ?city_obj ; 
        ocgml:attrName 'UNIQUE_ID';
        ocgml:strVal ?id .

    OPTIONAL { ?attr_1 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'SetbackFront' ;
        ocgml:strVal ?front_setback . 
    }

    OPTIONAL { ?attr_2 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'Storeys' ;
        ocgml:strVal ?storeys . 
    }

    OPTIONAL { ?attr_3 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'SetbackSide' ;
        ocgml:strVal ?side_setback . 
    }  

    OPTIONAL { ?attr_4 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'SetbackRear' ;
        ocgml:strVal ?rear_setback .
    }   

    OPTIONAL { ?attr_5 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'PartyWall' ;
        ocgml:strVal ?partywall_setback . 
    }  

    OPTIONAL { ?attr_6 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'GPR' ;
        ocgml:strVal ?gpr . 
    }

    OPTIONAL { ?attr_7 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'ExtRef' ;
        ocgml:uriVal ?ext_ref . 
    }

    OPTIONAL { ?attr_8 ocgml:cityObjectId ?city_obj ;
    ocgml:attrName 'LandUse' ;
    ocgml:strVal ?landuse . 
    }

    OPTIONAL { ?attr_9 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'NAME' ;
        ocgml:strVal ?name . 
    }

    OPTIONAL { ?attr_10 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'AllowedProgrammes' ;
        ocgml:strVal ?allowed_programmes . 
    } 
    } } 
    """

    qr = get_query_result(endpoint, q)

    sbp_dataset = TripleDataset(out_dir)

    for i in qr.index:
        sbp_dataset.create_street_block_plan_triples(qr.loc[i, 'city_obj'],
                                                     qr.loc[i, 'storeys'],
                                                     qr.loc[i, 'front_setback'],
                                                     qr.loc[i, 'side_setback'],
                                                     qr.loc[i, 'rear_setback'],
                                                     qr.loc[i, 'partywall_setback'],
                                                     qr.loc[i, 'ext_ref'],
                                                     qr.loc[i, 'name'],
                                                     qr.loc[i, 'landuse'],
                                                     qr.loc[i, 'gpr'],
                                                     qr.loc[i, 'allowed_programmes'])
    sbp_dataset.write_triples("street_block_plans_triples", out_endpoint)

    logger.info(f"{len(qr)} Street Block Plan items have been instantiated.")


def instantiate_urban_design_areas(endpoint, out_dir, out_endpoint=None):
    """
    Query and instantiate Urban Design Areas regulation content.

    :param endpoint: SPARQL endpoint to query urban design areas data.
    :param out_dir: Directory to write the output N-Quads file.
    :param out_endpoint: SPARQL endpoint to upload the generated triples.
    """

    q = """
    PREFIX ocgml:<http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
    SELECT ?city_obj ?name ?ext_ref
    WHERE {GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobjectgenericattrib/> 
    { 
    ?genAttr ocgml:cityObjectId ?city_obj .
    ?genAttr ocgml:attrName 'Name' ;
    ocgml:strVal ?name .

    ?genAttr1 ocgml:cityObjectId ?city_obj .
    ?genAttr1 ocgml:attrName 'ExtRef' ;
    ocgml:uriVal ?ext_ref . 
    } } 
    """

    qr = get_query_result(endpoint, q)
    uda = TripleDataset(out_dir)
    for i in qr.index:
        uda.create_urban_design_areas_triples(qr.loc[i, 'city_obj'], str(qr.loc[i, 'ext_ref']), qr.loc[i, 'name'])
    uda.write_triples("urban_design_areas_triples", out_endpoint)

    logger.info(f"{len(qr)} Urban Design Area items have been instantiated.")


def instantiate_urban_design_guidelines(endpoint, uda, out_dir, out_endpoint=None):
    """
    Queries and instantiates Urban Design Guidelines regulation content.

    :param endpoint: SPARQL endpoint to query urban design guidelines data.
    :param uda: urban design areas data to be linked to urban design guidelines data.
    :param out_dir: Directory to write the output N-Quads file.
    :param out_endpoint: SPARQL endpoint to upload the generated triples.
    """

    q = """
    PREFIX ocgml: <http://www.theworldavatar.com/ontology/ontocitygml/citieskg/OntoCityGML.owl#>
    SELECT ?city_obj ?additional_type ?ext_ref ?partywall ?setback ?area ?height
    WHERE {GRAPH <http://www.theworldavatar.com:83/citieskg/namespace/singaporeEPSG4326/sparql/cityobjectgenericattrib/> 
    {

    ?attr0 ocgml:cityObjectId ?city_obj ;
    ocgml:attrName 'TYPE';
    ocgml:strVal 'URBAN_DESIGN_GUIDELINES' . 

    OPTIONAL { ?attr1 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'Type' ;
        ocgml:strVal ?additional_type . 
    }

    OPTIONAL { ?attr2 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'ExtRef' ;
        ocgml:uriVal ?ext_ref . 
    }

    OPTIONAL { ?attr3 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'PartyWall' ;
        ocgml:strVal ?partywall . 
    }

    OPTIONAL { ?attr4 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'Setback' ;
        ocgml:strVal ?setback . 
    }

    OPTIONAL { ?attr5 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'Urban_Design_Area' ;
        ocgml:strVal ?area . 
    }

    OPTIONAL { ?attr6 ocgml:cityObjectId ?city_obj ;
        ocgml:attrName 'Storeys' ;
        ocgml:strVal ?height . 
    } 
    } }
    """

    udg = get_query_result(endpoint, q)
    udg_dataset = TripleDataset(out_dir)
    for i in udg.index:
        udg_dataset.create_urban_design_guidelines_triples(udg.loc[i, 'city_obj'],
                                                           str(udg.loc[i, 'ext_ref']),
                                                           udg.loc[i, 'partywall'],
                                                           udg.loc[i, 'height'],
                                                           udg.loc[i, 'setback'],
                                                           udg.loc[i, 'additional_type'],
                                                           udg.loc[i, 'area'],
                                                           uda)
    udg_dataset.write_triples("urban_design_guidelines_triples", out_endpoint)

    logger.info(f"{len(udg)} Urban Design Guideline items have been instantiated.")


def instantiate_development_control_plans(cp, lha, pb, out_dir, out_endpoint=None):
    """
    Queries and Instantiates Development Control Plans regulation content.
     :param cp: DataFrame containing development control plans.
     :param lha: URIs of landed housing areas.
     :param pb: URIs of planning boundaries.
     :param out_dir: Directory to save the output N-Quads file.
     :param out_endpoint: Optional SPARQL Update endpoint URL for uploading triples.
    """

    dataset = TripleDataset(out_dir)

    road_buff_15_uris = dataset.create_road_category_triples(GFAOntoManager.ROAD_BUFFER_15)
    road_buff_30_uris = dataset.create_road_category_triples(GFAOntoManager.ROAD_BUFFER_30, 6, GFAOntoManager.MINIMUM)
    road_buff_24_uris = dataset.create_road_category_triples(GFAOntoManager.ROAD_BUFFER_24, 5, GFAOntoManager.MAXIMUM)
    road_buff_7_uris = dataset.create_road_category_triples(GFAOntoManager.ROAD_BUFFER_7)
    road_buff_2_uris = dataset.create_road_category_triples(GFAOntoManager.ROAD_BUFFER_2)
    road_buff_0_uris = dataset.create_road_category_triples(GFAOntoManager.ROAD_BUFFER_0)

    for i in cp.index:
        parameters = {'zone': cp.loc[i, 'zone'].split(';'),
                      'for_programme': cp.loc[i, 'for_programme'],
                      'setback': cp.loc[i, 'setback'],
                      'storeys': cp.loc[i, 'storeys'],
                      'floor_height': cp.loc[i, 'floor_height'],
                      'site_coverage': cp.loc[i, 'site_coverage'],
                      'site_area': cp.loc[i, 'site_area'],
                      'avg_width': cp.loc[i, 'avg_width'],
                      'avg_depth': cp.loc[i, 'avg_depth'],
                      'gpr': cp.loc[i, 'gpr'],
                      'max_gfa': cp.loc[i, 'max_gfa'],
                      'for_neighbour_zone_type': cp.loc[i, 'for_neighbour_zone_type'],
                      'abuts_1_3_road_category': cp.loc[i, 'abuts_1_3_road_category'],
                      'abuts_GCBA': cp.loc[i, 'abuts_GCBA'],
                      'in_GCBA': cp.loc[i, 'in_GCBA'],
                      'for_corner_plot': cp.loc[i, 'for_corner_plot'],
                      'for_fringe_plot': cp.loc[i, 'for_fringe_plot'],
                      'in_landed_housing_area': cp.loc[i, 'in_landed_housing_area'],
                      'in_planning_boundary': cp.loc[i, 'in_planning_boundary'],
                      'in_central_area': cp.loc[i, 'in_central_area'],
                      'ext_ref': cp.loc[i, 'ext_ref']}

        if cp.loc[i, 'zone'] == 'EducationalInstitution':
            dataset.create_control_plan_triples(parameters, lha, pb, (road_buff_30_uris + road_buff_24_uris))

        elif cp.loc[i, 'for_programme'] in (
                'ServicedApartmentResidentialZone', 'ServicedApartmentMixedUseZone') and not pd.isna(
            cp.loc[i, 'in_planning_boundary']):
            dataset.create_control_plan_triples(parameters, lha, pb, road_buff_7_uris)

        elif cp.loc[i, 'for_programme'] in (
                'ServicedApartmentResidentialZone', 'ServicedApartmentMixedUseZone') and not pd.isna(
            cp.loc[i, 'in_planning_boundary']):
            dataset.create_control_plan_triples(parameters, lha, pb, road_buff_30_uris)

        elif cp.loc[i, 'for_programme'] == 'TerraceType2':
            dataset.create_control_plan_triples(parameters, lha, pb, road_buff_2_uris)

        elif cp.loc[i, 'for_programme'] in ('Bungalow', 'GoodClassBungalow', 'Semi-DetachedHouse', 'TerraceType1'):
            dataset.create_control_plan_triples(parameters, lha, pb, road_buff_24_uris)

        elif cp.loc[i, 'for_programme'] == 'Flat':
            if not pd.isna(cp.loc[i, 'in_planning_boundary']):
                if (pd.isna(cp.loc[i, 'in_GCBA'])) and (cp.loc[i, 'in_central_area'] > 0):
                    dataset.create_control_plan_triples(parameters, lha, pb, road_buff_0_uris)
                else:
                    dataset.create_control_plan_triples(parameters, lha, pb, (road_buff_7_uris + road_buff_15_uris))
            else:
                if (pd.isna(cp.loc[i, 'in_GCBA'])) and (cp.loc[i, 'in_central_area'] > 0):
                    dataset.create_control_plan_triples(parameters, lha, pb, road_buff_0_uris)
                else:
                    dataset.create_control_plan_triples(parameters, lha, pb, (road_buff_30_uris + road_buff_15_uris))

        elif cp.loc[i, 'for_programme'] == 'Condominium':
            if not pd.isna(cp.loc[i, 'in_planning_boundary']):
                dataset.create_control_plan_triples(parameters, lha, pb, (road_buff_7_uris + road_buff_15_uris))
            else:
                dataset.create_control_plan_triples(parameters, lha, pb, road_buff_30_uris)
        else:
            dataset.create_control_plan_triples(parameters, lha, pb, road_buff_15_uris)

    dataset.write_triples("control_plans_triples", out_endpoint)

    logger.info(f"{len(cp)} Control Development Plans items have been instantiated.")


def instantiate_type_regulation_overlaps(type_regs, out_dir, out_endpoint=None):
    """
    Write generated triples representing the overlap between plots and type-based regulations into an N-Quads file.

    :param type_regs: DataFrame containing type-based regulations and their associated plots.
    :param out_dir: Directory where the generated N-Quads file will be written.
    :param out_endpoint: SPARQL endpoint URL for uploading the generated triples (optional).
    """

    type_regulation_links = TripleDataset(out_dir)
    type_regulation_links.create_type_regulation_overlap_triples(type_regs)

    type_regulation_links.write_triples("type_regulation_overlap_triples", out_endpoint)

    logger.info('N-quads for type regulation and plot overlap instantiated.')


def instantiate_area_regulation_overlaps(endpoint_plots,
                                         endpoint_central_area,
                                         endpoint_design_urban_area,
                                         endpoint_conservation,
                                         endpoint_monument,
                                         endpoint_planning_boundaries,
                                         endpoint_height_control,
                                         endpoint_landed_housing,
                                         endpoint_street_block_plan,
                                         endpoint_urban_design_guidelines,
                                         out_dir,
                                         out_endpoint=None
                                         ):
    """
    Instantiates triples for regulation overlaps by querying multiple regulation SPARQL endpoints
    and generating overlaps with provided plot data.

    :param endpoint_plots: SPARQL endpoint URL for retrieving plot data.
    :param endpoint_central_area: SPARQL endpoint URL for the central area regulations.
    :param endpoint_design_urban_area: SPARQL endpoint URL for the urban design area regulations.
    :param endpoint_conservation: SPARQL endpoint URL for the conservation regulations.
    :param endpoint_monument: SPARQL endpoint URL for monument regulations.
    :param endpoint_planning_boundaries: SPARQL endpoint URL for planning boundaries regulations.
    :param endpoint_height_control: SPARQL endpoint URL for height control regulations.
    :param endpoint_landed_housing: SPARQL endpoint URL for landed housing regulations.
    :param endpoint_street_block_plan: SPARQL endpoint URL for street block plan regulations.
    :param endpoint_urban_design_guidelines: SPARQL endpoint URL for urban design guidelines regulations.
    :param out_dir: Directory where the generated triples will be saved as N-Quads.
    :param out_endpoint: (Optional) SPARQL Update endpoint URL to upload the generated triples.
    """
    plots = get_plots(endpoint_plots)
    logger.info(f"{len(plots)} plot items have been retrieved.")

    regs = []
    regs.append(get_regulation_overlaps(endpoint_central_area, plots, 0.4, False))
    regs.append(get_regulation_overlaps(endpoint_design_urban_area, plots, 0.4, False))
    regs.append(get_regulation_overlaps(endpoint_conservation, plots, 0.4, False))
    regs.append(get_regulation_overlaps(endpoint_monument, plots, 0.005, False))
    regs.append(get_regulation_overlaps(endpoint_planning_boundaries, plots, 0, True))
    regs.append(get_regulation_overlaps(endpoint_height_control, plots, 0.01, False))
    regs.append(get_regulation_overlaps(endpoint_landed_housing, plots, 0.01, False))
    regs.append(get_regulation_overlaps(endpoint_street_block_plan, plots, 0.4, False))
    regs.append(get_regulation_overlaps(endpoint_urban_design_guidelines, plots, 0.01, False))

    regulation_overlaps = TripleDataset(out_dir)

    for reg in regs:
        for i in reg.index:
            regulation_overlaps.create_area_regulation_overlap_triples(reg.loc[i, 'obj_id'], reg.loc[i, 'plots'])

    regulation_overlaps.write_triples("area_regulation_overlap_triples", out_endpoint)

    logger.info("N-quads for area regulation overlaps with plots instantiated.")


'''------------------------------------INSTANTIATE PLOT PROPERTIES---------------------------------'''


def instantiate_plot_property_triples(plots, road_network, out_dir, out_endpoint=None):
    """
    Writes generated residential and road plot property triples into an N-Quads file.

    :param plots: GeoDataFrame containing general plot data.
    :param road_network: GeoDataFrame containing road network data.
    :param out_dir: Directory where the generated N-Quads file will be written.
    :param out_endpoint: Optional; SPARQL endpoint URL for uploading the generated triples.
    """

    plot_properties = TripleDataset(out_dir)

    neighbor_links = get_neighbor_links(plots)
    plot_properties.create_neighbor_triples(neighbor_links)
    logger.info('Plot neighbor triples created')

    plot_properties.create_site_area_triples(plots)
    logger.info('Plot site area triples created.')

    res_plots = plots[plots['zone'].isin(['RESIDENTIAL',
                                          'RESIDENTIAL / INSTITUTION',
                                          'COMMERCIAL & RESIDENTIAL',
                                          'RESIDENTIAL WITH COMMERCIAL AT 1ST STOREY',
                                          'WHITE',
                                          'BUSINESS PARK - WHITE',
                                          'BUSINESS 1 - WHITE',
                                          'BUSINESS 2 - WHITE'])]
    plot_properties.create_residential_plot_property_triples(res_plots, plots)
    logger.info('Residential plot property triples created.')

    road_plots = plots[plots['zone'].isin(['ROAD'])]
    plot_properties.create_road_plot_property_triples(road_network, road_plots)
    logger.info('Road plot properties triples created.')

    plot_properties.write_triples("plot_properties_triples", out_endpoint)
    logger.info('Plot property triples instantiated.')


def instantiate_allowed_gfa(df, out_dir, out_endpoint=None):
    """
    :param df: A DataFrame containing the columns 'plots' and 'gfa' to generate triples.
    :param out_dir: Path to the directory where the triple files will be written.
    :param out_endpoint: Endpoint to associate with the generated triples. Default is None.

    """

    dataset = TripleDataset(out_dir)

    for i in df.index:
        dataset.create_allowed_gfa_triples(df.loc[i, 'plots'], df.loc[i, 'gfa'])

    dataset.write_triples("gfa_triples", out_endpoint)

    logger.info("Allowed gfa triples created and n-quads instantiated.")
