import uuid
import pandas as pd
import logging
from rdflib.namespace import RDF, XSD
from GFAOntoManager import *
from rdflib import Dataset, Literal, URIRef
from SPARQLWrapper import SPARQLWrapper, POST
from utils import set_road_plot_properties, set_residential_plot_properties

logger = logging.getLogger(__name__)


class TripleDataset:
    def __init__(self, out_dir):
        """
        Initialize a new TripleDataset instance.

        :param out_dir: Directory to which the output files will be written.
        """
        self.dataset = Dataset()
        self.out_dir = out_dir

    def create_absolute_height_triples(self, city_obj, height):
        """
        Generates triples to represent an Absolute Height and adds them to the dataset.

        :param city_obj: URI of the city object.
        :param height: Literal height value.
        """

        absolute_height = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        measure = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))

        self.dataset.add((city_obj, GFAOntoManager.ALLOWS_ABSOLUTE_HEIGHT, absolute_height,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((absolute_height, RDF.type, GFAOntoManager.ABSOLUTE_HEIGHT,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((absolute_height, GFAOntoManager.HAS_AGGREGATE_FUNCTION, GFAOntoManager.MAXIMUM,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((absolute_height, GFAOntoManager.HAS_VALUE, measure,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((measure, RDF.type, GFAOntoManager.MEASURE,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((measure, GFAOntoManager.HAS_UNIT, GFAOntoManager.METRE,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((GFAOntoManager.METRE, RDF.type, GFAOntoManager.LENGTH_UNIT,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((measure, GFAOntoManager.HAS_NUMERIC_VALUE, height,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_floor_height_triples(self, city_obj, floor_height):
        """
        Generates triples to represent a Floor To Floor Height and adds them to the dataset.

        :param city_obj: URI of the city object.
        :param floor_height: Literal floor height value.
        """
        floor_height_uri = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        measure = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))

        self.dataset.add((city_obj, GFAOntoManager.REQUIRES_FLOOR_TO_FLOOR_HEIGHT, floor_height_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((floor_height_uri, RDF.type, GFAOntoManager.FLOOR_TO_FLOOR_HEIGHT,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add(
            (floor_height_uri, GFAOntoManager.HAS_VALUE, measure, GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add(
            (measure, GFAOntoManager.HAS_UNIT, GFAOntoManager.METRE, GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add(
            (measure, GFAOntoManager.HAS_NUMERIC_VALUE, floor_height, GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_storey_aggregate_triples(self, city_obj, storey_aggregate, height_value,
                                        function=GFAOntoManager.MAXIMUM):
        """
        Generates triples to represent a Storey Aggregate and adds them to the dataset.

        :param city_obj: URI of the city object.
        :param storey_aggregate: URI for the storey aggregate.
        :param height_value: Literal value representing the number of storeys.
        :param function: Aggregate function to be used (e.g. GFAOntoManager.MAXIMUM).
        """
        self.dataset.add((city_obj, GFAOntoManager.ALLOWS_STOREY_AGGREGATE, storey_aggregate,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((storey_aggregate, RDF.type, GFAOntoManager.STOREY_AGGREGATE,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((storey_aggregate, GFAOntoManager.HAS_AGGREGATE_FUNCTION, function,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((storey_aggregate, GFAOntoManager.NUMBER_OF_STOREYS, height_value,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_height_control_triples(self, city_object_uri, ext_ref, height, unit):
        """
        Generates triples to represent Height Control Plans and adds them to the dataset.

        :param city_object_uri: URI of the city object.
        :param ext_ref: External reference for the city object.
        :param height: Height value as a string or numeric.
        :param unit: Type of unit for the height control (e.g., 'NUMBER OF STOREYS', 'METRES BASED ON SHD').
        """
        height_control_plan = URIRef(city_object_uri)
        external_reference_uri = URIRef(ext_ref)
        storey_aggregate = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        height_value = Literal(str(height), datatype=XSD.integer)

        self.dataset.add((height_control_plan, RDF.type, GFAOntoManager.HEIGHT_CONTROL_PLAN,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((height_control_plan, GFAOntoManager.HAS_EXTERNAL_REF, external_reference_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

        if 'NUMBER OF STOREYS' in unit:
            self.create_storey_aggregate_triples(height_control_plan, storey_aggregate, height_value)
            i = 1
            while i <= int(height):
                storey = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
                self.dataset.add((storey_aggregate, GFAOntoManager.CONTAINS_STOREY, storey,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.dataset.add(
                    (storey, RDF.type, GFAOntoManager.STOREY, GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.dataset.add((storey, GFAOntoManager.AT_LEVEL, Literal(str(i), datatype=XSD.integer),
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                i += 1

        elif 'METRES BASED ON SHD' in unit:
            self.create_absolute_height_triples(height_control_plan, height_value)
        else:
            self.dataset.add((height_control_plan, GFAOntoManager.HAS_ADDITIONAL_TYPE, GFAOntoManager.DETAILED_CONTROL,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_conservation_triples(self, city_obj, ext_ref):
        """
        Generates the necessary RDF triples to represent a Conservation Area and adds them to the triple dataset.

        :param city_obj: A string containing the URI of the city object to be defined as a Conservation Area.
        :param ext_ref: A string containing the external reference URI associated with the city object.
        """

        conservation_area = URIRef(city_obj)
        external_reference_uri = URIRef(ext_ref)

        self.dataset.add((conservation_area, RDF.type, GFAOntoManager.CONSERVATION_AREA,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((conservation_area, GFAOntoManager.HAS_EXTERNAL_REF, external_reference_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_central_area_triples(self, city_obj, ext_ref):
        """
        Generates necessary RDF triples to represent a city object as a Central Area
        and adds them to the dataset.

        :param city_obj: The URI of the city object to be defined as a Central Area.
        :param ext_ref: The external reference URI associated with the city object.
        """

        central_area = URIRef(city_obj)
        external_reference_uri = URIRef(ext_ref)

        self.dataset.add(
            (central_area, RDF.type, GFAOntoManager.CENTRAL_AREA, GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((central_area, GFAOntoManager.HAS_EXTERNAL_REF, external_reference_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_planning_boundaries_triples(self,
                                           city_obj,
                                           ext_ref,
                                           planning_boundary_name,
                                           region):
        """
        Generates the necessary triples to represent a Planning Boundary and adds them to the triple dataset.

        :param city_obj: A string containing the URI of the city object to be defined as a Planning Boundary.
        :param ext_ref: A string containing the external reference URI associated with the city object.
        :param planning_boundary_name: The name of the planning boundary.
        :param region: The name of the region (one of 'EAST REGION', 'CENTRAL REGION', 'WEST REGION',
                   'NORTH-EAST REGION', or 'NORTH REGION').
        :return: None
        """

        planning_boundary = URIRef(city_obj)
        external_reference_uri = URIRef(ext_ref)
        planning_boundary_name = Literal(str(planning_boundary_name), datatype=XSD.string)
        regions = {'EAST REGION': URIRef(GFAOntoManager.ONTO_PLANNING_REG_PREFIX + 'EastRegion'),
                   'CENTRAL REGION': URIRef(GFAOntoManager.ONTO_PLANNING_REG_PREFIX + 'CentralRegion'),
                   'WEST REGION': URIRef(GFAOntoManager.ONTO_PLANNING_REG_PREFIX + 'WestRegion'),
                   'NORTH-EAST REGION': URIRef(GFAOntoManager.ONTO_PLANNING_REG_PREFIX + 'NorthEastRegion'),
                   'NORTH REGION': URIRef(GFAOntoManager.ONTO_PLANNING_REG_PREFIX + 'NorthRegion')}

        self.dataset.add((planning_boundary, RDF.type, GFAOntoManager.PLANNING_BOUNDARY,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((planning_boundary, GFAOntoManager.HAS_EXTERNAL_REF, external_reference_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((planning_boundary, GFAOntoManager.HAS_NAME, planning_boundary_name,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((planning_boundary, GFAOntoManager.IS_PART_OF_OPR, regions[region],
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((regions[region], RDF.type, GFAOntoManager.PLANNING_REGION,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_monument_triples(self, city_obj, ext_ref, name):
        """
        Generates the necessary triples to represent a Monument and adds them to the triple dataset.

        :param city_obj: A string representing the URI of the city object to be defined as a Monument.
        :param ext_ref: A string representing the external reference URI associated with the city object.
        :param name: The name of the Monument.
        """

        monument = URIRef(city_obj)
        external_reference_uri = URIRef(ext_ref)
        monument_name = Literal(str(name), datatype=XSD.string)

        self.dataset.add((monument, RDF.type, GFAOntoManager.MONUMENT,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((monument, GFAOntoManager.HAS_EXTERNAL_REF, external_reference_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((monument, GFAOntoManager.HAS_NAME, monument_name,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_landed_housing_areas_triples(self,
                                            city_obj,
                                            ext_ref,
                                            height,
                                            lha_type,
                                            area):
        """
        Generates triples to represent Landed Housing Areas and adds them to the triple dataset.

        :param city_obj: URI of the city object.
        :param ext_ref: External reference for the city object.
        :param height: Height (in storeys).
        :param lha_type: Type of landed housing.
        :param area: The area classification (e.g., 'GOOD CLASS BUNGALOW AREA', 'LANDED HOUSING AREA').
        """

        landed_housing_area = URIRef(city_obj)
        external_reference_uri = URIRef(ext_ref)
        height_value = Literal(str(height), datatype=XSD.integer)
        storey_aggregate = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        good_class_bungalow = URIRef(GFAOntoManager.ONTO_ZONING_URI_PREFIX + "GoodClassBungalow")
        semi_detached_house = URIRef(GFAOntoManager.ONTO_ZONING_URI_PREFIX + "Semi-DetachedHouse")
        bungalow = URIRef(GFAOntoManager.ONTO_ZONING_URI_PREFIX + "Bungalow")
        terrace_type_1 = URIRef(GFAOntoManager.ONTO_ZONING_URI_PREFIX + "TerraceType1")
        terrace_type_2 = URIRef(GFAOntoManager.ONTO_ZONING_URI_PREFIX + "TerraceType2")

        self.dataset.add((landed_housing_area, GFAOntoManager.HAS_EXTERNAL_REF, external_reference_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.create_storey_aggregate_triples(landed_housing_area, storey_aggregate, height_value)
        i = 1

        while i <= int(height):
            storey = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
            self.dataset.add((storey_aggregate, GFAOntoManager.CONTAINS_STOREY, storey,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            self.dataset.add((storey, RDF.type, GFAOntoManager.STOREY,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            self.dataset.add((storey, GFAOntoManager.AT_LEVEL, Literal(str(i), datatype=XSD.integer),
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            i += 1

        if 'GOOD CLASS BUNGALOW AREA' in area:
            self.dataset.add((landed_housing_area, RDF.type, GFAOntoManager.GOOD_CLASS_BUNGALOW_AREA,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            self.dataset.add((landed_housing_area, GFAOntoManager.ALLOWS_PROGRAMME, good_class_bungalow,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

        elif 'LANDED HOUSING AREA' in area:
            if 'SEMI-DETACHED' in lha_type:
                self.dataset.add((landed_housing_area, RDF.type, GFAOntoManager.LANDED_HOUSING_AREA,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.dataset.add((landed_housing_area, GFAOntoManager.ALLOWS_PROGRAMME, semi_detached_house,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.dataset.add((landed_housing_area, GFAOntoManager.ALLOWS_PROGRAMME, bungalow,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            elif 'BUNGALOWS' in lha_type:
                self.dataset.add((landed_housing_area, RDF.type, GFAOntoManager.LANDED_HOUSING_AREA,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.dataset.add((landed_housing_area, GFAOntoManager.ALLOWS_PROGRAMME, bungalow,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            elif 'MIXED LANDED' in lha_type:
                self.dataset.add((landed_housing_area, RDF.type, GFAOntoManager.LANDED_HOUSING_AREA,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.dataset.add((landed_housing_area, GFAOntoManager.ALLOWS_PROGRAMME, bungalow,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.dataset.add((landed_housing_area, GFAOntoManager.ALLOWS_PROGRAMME, semi_detached_house,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.dataset.add((landed_housing_area, GFAOntoManager.ALLOWS_PROGRAMME, terrace_type_1,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.dataset.add((landed_housing_area, GFAOntoManager.ALLOWS_PROGRAMME, terrace_type_2,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_setback_triples(self,
                               city_obj,
                               setback,
                               predicate_type,
                               setback_type,
                               setback_value):
        """
        Generates triples to represent a setback and adds them to the triple dataset.

        :param city_obj: URI of the city object.
        :param setback: URI for the setback object.
        :param predicate_type: The predicate used to link the city object and the setback.
        :param setback_type: The type/class of the setback (e.g., GFAOntoManager.FRONT_SETBACK).
        :param setback_value: The numeric value of the setback.
        """

        setback_measure = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        self.dataset.add((city_obj, predicate_type, setback,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((setback, RDF.type, setback_type,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((setback, GFAOntoManager.HAS_AGGREGATE_FUNCTION, GFAOntoManager.MINIMUM,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((setback, GFAOntoManager.HAS_VALUE, setback_measure,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((setback_measure, GFAOntoManager.HAS_UNIT, GFAOntoManager.METRE,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((setback_measure, GFAOntoManager.HAS_NUMERIC_VALUE,
                          Literal(str(setback_value), datatype=XSD.double),
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_setback_collection(self,
                                  city_obj,
                                  setbacks,
                                  storey,
                                  storey_level,
                                  predicate_type,
                                  setback_type,
                                  height_provided):
        """
        Generates triples for a collection of setbacks and adds them to the triple dataset.

        :param city_obj: URI of the city object.
        :param setbacks: A comma-separated string of setback values.
        :param storey: URI representing a storey.
        :param storey_level: The storey level (integer).
        :param predicate_type: The predicate used to relate the city object and the setback.
        :param setback_type: The type/class of the setback.
        :param height_provided: Boolean indicating if height data is provided.
        """

        if height_provided:
            if not pd.isna(setbacks):
                setbacks_list = setbacks.split(',')
                if len(setbacks_list) > 1:
                    current_setback_value = setbacks_list[storey_level - 1]
                else:
                    current_setback_value = setbacks_list[0]
                setback = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
                self.create_setback_triples(city_obj, setback, predicate_type, setback_type, current_setback_value)
                self.dataset.add((setback, GFAOntoManager.AT_STOREY, storey,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        else:
            if not pd.isna(setbacks):
                setbacks_list = setbacks.split(',')
                if len(setbacks_list) > 1:
                    for index, setback_value in enumerate(setbacks_list):
                        setback = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
                        storey = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
                        storey_level = index + 1
                        self.create_setback_triples(city_obj, setback, predicate_type, setback_type, setback_value)
                        self.dataset.add((storey, GFAOntoManager.AT_LEVEL, Literal(str(storey_level)),
                                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                else:
                    current_setback_value = setbacks_list[0]
                    setback = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
                    self.create_setback_triples(city_obj, setback, predicate_type, setback_type, current_setback_value)

    def create_street_block_plan_triples(self,
                                         city_obj,
                                         height,
                                         front_setback,
                                         side_setback,
                                         rear_setback,
                                         partywall_setback,
                                         ext_ref,
                                         name,
                                         landuse,
                                         gpr,
                                         allowed_programmes):
        """
        Generates triples to represent Street Block Plans and adds them to the triple dataset.

        :param city_obj: URI of the city object.
        :param height: Height (in storeys).
        :param front_setback: Comma-separated setback values for the front setback.
        :param side_setback: Comma-separated setback values for the side setback.
        :param rear_setback: Comma-separated setback values for the rear setback.
        :param partywall_setback: Numeric value for the partywall setback.
        :param ext_ref: External reference for the city object.
        :param name: Name of the Street Block Plan.
        :param landuse: Land use type.
        :param gpr: Gross Plot Ratio (GPR) value.
        :param allowed_programmes: Comma-separated allowed programmes.
        """

        street_block_plan = URIRef(city_obj)
        self.dataset.add((street_block_plan, RDF.type, GFAOntoManager.STREET_BLOCK_PLAN,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        storey_aggregate = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))

        if not str(ext_ref):
            ext_ref_uri = URIRef(ext_ref.strip())
            self.dataset.add((street_block_plan, GFAOntoManager.HAS_EXTERNAL_REF, ext_ref_uri,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

        if not pd.isna(name):
            self.dataset.add((street_block_plan, GFAOntoManager.HAS_NAME, Literal(str(name), datatype=XSD.string),
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

        if not pd.isna(landuse):
            zone = URIRef(str(GFAOntoManager.ONTO_ZONING_URI_PREFIX + landuse.strip()))
            self.dataset.add((street_block_plan, GFAOntoManager.HAS_ZONE_TYPE, zone,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

        if not pd.isna(gpr):
            gpr_uri = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
            self.dataset.add((street_block_plan, GFAOntoManager.ALLOWS_GROSS_PLOT_RATIO, gpr_uri,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            self.dataset.add((gpr_uri, RDF.type, GFAOntoManager.GROSS_PLOT_RATIO,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            self.dataset.add((gpr_uri, GFAOntoManager.HAS_VALUE_OPR, Literal(str(gpr), datatype=XSD.double),
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

        if (not pd.isna(allowed_programmes)) and (not 'Existing' in allowed_programmes.split(',')):
            programmes = allowed_programmes.split(',')
            for i in programmes:
                self.dataset.add((street_block_plan, GFAOntoManager.ALLOWS_PROGRAMME,
                                  URIRef(GFAOntoManager.ONTO_ZONING_URI_PREFIX + i),
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(height):
            self.create_storey_aggregate_triples(street_block_plan, storey_aggregate,
                                                 Literal(str(height), datatype=XSD.integer))
            storey_level = 1
            while storey_level <= int(height):
                storey = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
                self.dataset.add((storey_aggregate, GFAOntoManager.CONTAINS_STOREY, storey,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.dataset.add((storey, RDF.type, GFAOntoManager.STOREY,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.dataset.add((storey, GFAOntoManager.AT_LEVEL, Literal(str(storey_level), datatype=XSD.integer),
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.create_setback_collection(street_block_plan, front_setback, storey, storey_level,
                                               GFAOntoManager.REQUIRES_SETBACK, GFAOntoManager.FRONT_SETBACK, True)
                self.create_setback_collection(street_block_plan, side_setback, storey, storey_level,
                                               GFAOntoManager.REQUIRES_SETBACK, GFAOntoManager.SIDE_SETBACK, True)
                self.create_setback_collection(street_block_plan, rear_setback, storey, storey_level,
                                               GFAOntoManager.REQUIRES_SETBACK, GFAOntoManager.REAR_SETBACK, True)
                if (not pd.isna(partywall_setback)) and bool(int(partywall_setback)):
                    partywall = '0.0'
                    self.create_setback_collection(street_block_plan, partywall, storey, storey_level,
                                                   GFAOntoManager.REQUIRES_PARTYWALL, GFAOntoManager.PARTYWALL, True)
                storey_level += 1
        else:
            storey = 'nan'
            storey_level = 'nan'
            self.create_setback_collection(street_block_plan, front_setback, storey, storey_level,
                                           GFAOntoManager.REQUIRES_SETBACK, GFAOntoManager.FRONT_SETBACK, False)
            self.create_setback_collection(street_block_plan, side_setback, storey, storey_level,
                                           GFAOntoManager.REQUIRES_SETBACK, GFAOntoManager.SIDE_SETBACK, False)
            self.create_setback_collection(street_block_plan, rear_setback, storey, storey_level,
                                           GFAOntoManager.REQUIRES_SETBACK, GFAOntoManager.REAR_SETBACK, False)
            if (not pd.isna(partywall_setback)) and bool(int(partywall_setback)):
                partywall = '0.0'
                self.create_setback_collection(street_block_plan, partywall, storey, storey_level,
                                               GFAOntoManager.REQUIRES_PARTYWALL, GFAOntoManager.PARTYWALL, False)

    def create_urban_design_areas_triples(self, city_obj, ext_ref, name):
        """
        Generates triples to represent Urban Design Areas and adds them to the triple dataset.

        :param city_obj: URI of the city object.
        :param ext_ref: External reference for the city object.
        :param name: Name of the Urban Design Area.
        """

        urban_design_area = URIRef(city_obj)
        ext_ref_uri = URIRef(ext_ref.strip())

        self.dataset.add((urban_design_area, RDF.type, GFAOntoManager.URBAN_DESIGN_AREA,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((urban_design_area, GFAOntoManager.HAS_EXTERNAL_REF, ext_ref_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((urban_design_area, GFAOntoManager.HAS_NAME, Literal(str(name.strip()), datatype=XSD.string),
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_urban_design_guidelines_triples(self,
                                               city_obj,
                                               ext_ref,
                                               partywall_setback,
                                               height,
                                               setback,
                                               additional_type,
                                               area,
                                               urban_design_areas):
        """
        Generates triples to represent the Urban Design Guidelines and adds them to the triple dataset.

        :param city_obj: URI of the city object.
        :param ext_ref: External reference for the city object.
        :param partywall_setback: Party wall setback value.
        :param height: Height (in storeys).
        :param setback: Setback value.
        :param additional_type: Additional type classification.
        :param area: Area classification.
        :param urban_design_areas: Mapping of area names to their URIs.
        """
        urban_design_guideline = URIRef(city_obj)
        ext_ref_uri = URIRef(str(ext_ref.strip()))

        self.dataset.add((urban_design_guideline, RDF.type, GFAOntoManager.URBAN_DESIGN_GUIDELINE,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((urban_design_guideline, GFAOntoManager.HAS_EXTERNAL_REF, ext_ref_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(additional_type):
            additional_type = URIRef(GFAOntoManager.ONTO_PLANNING_REG_PREFIX + additional_type.strip())
            self.dataset.add((urban_design_guideline, GFAOntoManager.HAS_ADDITIONAL_TYPE, additional_type,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(area):
            current_design_area = URIRef(urban_design_areas[area])
            self.dataset.add((urban_design_guideline, GFAOntoManager.IN_URBAN_DESIGN_AREA, current_design_area,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            self.dataset.add((current_design_area, RDF.type, GFAOntoManager.URBAN_DESIGN_AREA,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(height):
            storey_aggregate = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
            self.create_storey_aggregate_triples(urban_design_guideline, storey_aggregate,
                                                 Literal(str(height), datatype=XSD.integer))
            storey_level = 1
            while storey_level <= int(height):
                storey = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
                self.dataset.add((storey_aggregate, GFAOntoManager.CONTAINS_STOREY, storey,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.dataset.add(
                    (storey, RDF.type, GFAOntoManager.STOREY, GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.dataset.add((storey, GFAOntoManager.AT_LEVEL, Literal(str(storey_level), datatype=XSD.integer),
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.create_setback_collection(urban_design_guideline, setback, storey, storey_level,
                                               GFAOntoManager.REQUIRES_SETBACK, GFAOntoManager.SETBACK, True)
                if (not pd.isna(partywall_setback)) and bool(int(partywall_setback)):
                    partywall = '0.0'
                    self.create_setback_collection(urban_design_guideline, partywall, storey, storey_level,
                                                   GFAOntoManager.REQUIRES_PARTYWALL, GFAOntoManager.PARTYWALL, True)
                storey_level += 1
        else:
            storey = 'nan'
            storey_level = 'nan'
            self.create_setback_collection(urban_design_guideline, setback, storey, storey_level,
                                           GFAOntoManager.REQUIRES_SETBACK, GFAOntoManager.SETBACK, False)
            if (not pd.isna(partywall_setback)) and bool(int(partywall_setback)):
                partywall = '0.0'
                self.create_setback_collection(urban_design_guideline, partywall, storey, storey_level,
                                               GFAOntoManager.REQUIRES_PARTYWALL, GFAOntoManager.PARTYWALL, False)

    def create_allowed_site_area_triples(self, control_plan, site_area):
        """
        Generates triples to represent the site area and adds them to the triple dataset.

        :param control_plan: URI of the control plan.
        :param site_area: Numeric value of the site area.
        """

        site_area_uri = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        measure = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        self.dataset.add((site_area_uri, RDF.type, GFAOntoManager.SITE_AREA,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((control_plan, GFAOntoManager.REQUIRES_SITE_AREA, site_area_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((site_area_uri, GFAOntoManager.HAS_AGGREGATE_FUNCTION, GFAOntoManager.MINIMUM,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((site_area_uri, GFAOntoManager.HAS_VALUE, measure,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((site_area_uri, GFAOntoManager.HAS_UNIT, GFAOntoManager.SQUARE_PREFIXED_METRE,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((measure, GFAOntoManager.HAS_NUMERIC_VALUE, site_area,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_site_coverage_triples(self, control_plan, site_coverage_value):
        """
        Generates triples to represent the site coverage and adds them to the triple dataset.

        :param control_plan: URI of the control plan.
        :param site_coverage_value: Numeric value of the site coverage.
        """

        site_coverage_uri = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        self.dataset.add((site_coverage_uri, RDF.type, GFAOntoManager.SITE_COVERAGE,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((control_plan, GFAOntoManager.ALLOWS_SITE_COVERAGE, site_coverage_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((site_coverage_uri, GFAOntoManager.HAS_VALUE_OPR, site_coverage_value,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_gpr_triples(self, control_plan, gpr_value, function=None):
        """
        Generates triples to represent the Gross Plot Ratio (GPR) and adds them to the triple dataset.

        :param control_plan: URI of the control plan.
        :param gpr_value: Numeric value of the Gross Plot Ratio.
        :param function: Optional aggregate function for the GPR (e.g., minimum, maximum).
        """

        gpr_uri = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        self.dataset.add((gpr_uri, RDF.type, GFAOntoManager.GROSS_PLOT_RATIO,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((control_plan, GFAOntoManager.ALLOWS_GROSS_PLOT_RATIO, gpr_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((gpr_uri, GFAOntoManager.HAS_VALUE_OPR, gpr_value,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(function):
            self.dataset.add((gpr_uri, GFAOntoManager.HAS_AGGREGATE_FUNCTION, function,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_required_average_width_triples(self, control_plan, width):
        """
        Generates triples to represent the average width and adds them to the triple dataset.

        :param control_plan: URI of the control plan.
        :param width: Numeric value of the average width.
        """

        width_uri = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        measure = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        self.dataset.add((control_plan, GFAOntoManager.REQUIRES_WIDTH, width_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((width_uri, RDF.type, GFAOntoManager.AVERAGE_WIDTH,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((width_uri, GFAOntoManager.HAS_VALUE, measure,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((measure, GFAOntoManager.HAS_UNIT, GFAOntoManager.SQUARE_PREFIXED_METRE,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((measure, GFAOntoManager.HAS_NUMERIC_VALUE, width,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((width_uri, GFAOntoManager.HAS_AGGREGATE_FUNCTION, GFAOntoManager.MINIMUM,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_required_average_depth_triples(self, control_plan, depth):
        """
        Generates triples to represent the average depth and adds them to the triple dataset.

        :param control_plan: URI of the control plan.
        :param depth: Numeric value of the average depth.
        """

        depth_uri = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        measure = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        self.dataset.add((control_plan, GFAOntoManager.REQUIRES_DEPTH, depth_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((depth_uri, RDF.type, GFAOntoManager.AVERAGE_DEPTH,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((depth_uri, GFAOntoManager.HAS_VALUE, measure,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((measure, GFAOntoManager.HAS_UNIT, GFAOntoManager.SQUARE_PREFIXED_METRE,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((measure, GFAOntoManager.HAS_NUMERIC_VALUE, depth,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((depth_uri, GFAOntoManager.HAS_AGGREGATE_FUNCTION, GFAOntoManager.MINIMUM,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_gfa_triples(self, control_plan, gfa):
        """
        Generates triples to represent the Gross Floor Area (GFA) and adds them to the triple dataset.

        :param control_plan: URI of the control plan.
        :param gfa: Numeric value of the Gross Floor Area.
        """

        gfa_uri = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        measure = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        self.dataset.add((control_plan, GFAOntoManager.ALLOWS_GROSS_FLOOR_AREA, gfa_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((gfa_uri, RDF.type, GFAOntoManager.GROSS_FLOOR_AREA,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((gfa_uri, GFAOntoManager.HAS_AGGREGATE_FUNCTION, GFAOntoManager.MAXIMUM,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((gfa_uri, GFAOntoManager.HAS_VALUE, measure,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((measure, GFAOntoManager.HAS_UNIT, GFAOntoManager.SQUARE_PREFIXED_METRE,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        self.dataset.add((measure, GFAOntoManager.HAS_NUMERIC_VALUE, gfa,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_control_plan_triples(self,
                                    parameters,
                                    landed_housing_areas,
                                    planning_boundaries,
                                    road_buffer_uris):
        """
        Generates triples to represent non-residential development control plans and adds them to the triple dataset.

        :param parameters: Dictionary containing control plan parameters (e.g., zone, for_programme, etc.).
        :param landed_housing_areas: List of URIs representing landed housing areas.
        :param planning_boundaries: DataFrame containing planning boundary information,
        specifically with a 'name' and 'pa' columns.
        :param road_buffer_uris: List of URIs representing road buffer constraints.
        """

        control_plan = URIRef(GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH + str(uuid.uuid1()))

        self.dataset.add((control_plan, RDF.type, GFAOntoManager.DEVELOPMENT_CONTROL_PLAN,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        for i in parameters['zone']:
            zone_uri = URIRef(GFAOntoManager.ONTO_ZONING_URI_PREFIX + i)
            self.dataset.add((control_plan, GFAOntoManager.FOR_ZONING_TYPE, zone_uri,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(parameters['for_programme']):
            programme_uri = URIRef(GFAOntoManager.ONTO_ZONING_URI_PREFIX + parameters['for_programme'])
            self.dataset.add((control_plan, GFAOntoManager.FOR_PROGRAMME, programme_uri,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(parameters['abuts_GCBA']):
            self.dataset.add((control_plan, GFAOntoManager.PLOT_ABUTS_GOOD_CLASS_BUNGALOW_AREA,
                              Literal(str(True), datatype=XSD.boolean),
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(parameters['in_GCBA']):
            self.dataset.add((control_plan, GFAOntoManager.PLOT_IN_GOOD_CLASS_BUNGALOW_AREA,
                              Literal(str(True), datatype=XSD.boolean),
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(parameters['in_central_area']):
            if parameters['in_central_area'] == 1.0:
                self.dataset.add((control_plan, GFAOntoManager.PLOT_IN_CENTRAL_AREA,
                                  Literal(str(True), datatype=XSD.boolean),
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            else:
                self.dataset.add((control_plan, GFAOntoManager.PLOT_IN_CENTRAL_AREA,
                                  Literal(str(False), datatype=XSD.boolean),
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(parameters['abuts_1_3_road_category']):
            self.dataset.add((control_plan, GFAOntoManager.PLOT_ABUTS_1_3_ROAD_CATEGORY,
                              Literal(str(True), datatype=XSD.boolean),
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(parameters['for_neighbour_zone_type']):
            neighbour_list = parameters['for_neighbour_zone_type'].split(';')
            for i in neighbour_list:
                current_neighbour_zone = URIRef(GFAOntoManager.ONTO_ZONING_URI_PREFIX + i)
                self.dataset.add((control_plan, GFAOntoManager.FOR_NEIGHBOUR_ZONE_TYPE, current_neighbour_zone,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(parameters['for_corner_plot']):
            self.dataset.add((control_plan, GFAOntoManager.FOR_CORNER_PLOT, Literal(str(True), datatype=XSD.boolean),
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(parameters['for_fringe_plot']):
            self.dataset.add((control_plan, GFAOntoManager.FOR_FRINGE_PLOT, Literal(str(True), datatype=XSD.boolean),
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(parameters['in_landed_housing_area']):
            for i in landed_housing_areas:
                self.dataset.add((control_plan, GFAOntoManager.FOR_PLOT_CONTAINED_IN, URIRef(i),
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(parameters['in_planning_boundary']):
            for i in planning_boundaries[planning_boundaries['name'].isin(['ORCHARD', 'NEWTON', 'RIVER VALLEY'])]['pa']:
                self.dataset.add((control_plan, GFAOntoManager.FOR_PLOT_CONTAINED_IN, URIRef(i),
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
        if not pd.isna(parameters['setback']):
            setback_uri = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
            self.create_setback_triples(control_plan, setback_uri, GFAOntoManager.REQUIRES_SETBACK,
                                        GFAOntoManager.SETBACK,
                                        Literal(str(parameters['setback']), datatype=XSD.double))
        storey_aggregate = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        if not pd.isna(parameters['storeys']) and '>' in str(parameters['storeys']):
            self.create_storey_aggregate_triples(control_plan, storey_aggregate,
                                                 Literal(str(parameters['storeys'].replace('>', '')),
                                                         datatype=XSD.integer),
                                                 GFAOntoManager.MINIMUM)
        if not pd.isna(parameters['storeys']) and '>' not in str(parameters['storeys']):
            self.create_storey_aggregate_triples(control_plan, storey_aggregate,
                                                 Literal(str(parameters['storeys']), datatype=XSD.integer))
        if not pd.isna(parameters['floor_height']):
            self.create_floor_height_triples(control_plan,
                                             Literal(str(parameters['floor_height']), datatype=XSD.double))
        if not pd.isna(parameters['site_coverage']):
            self.create_site_coverage_triples(control_plan,
                                              Literal(str(parameters['site_coverage']), datatype=XSD.double))
        if not pd.isna(parameters['site_area']):
            self.create_allowed_site_area_triples(control_plan,
                                                  Literal(str(parameters['site_area']), datatype=XSD.double))
        if not pd.isna(parameters['avg_width']):
            self.create_required_average_width_triples(control_plan,
                                                       Literal(str(parameters['avg_width']), datatype=XSD.double))
        if not pd.isna(parameters['avg_depth']):
            self.create_required_average_depth_triples(control_plan,
                                                       Literal(str(parameters['avg_depth']), datatype=XSD.double))
        if not pd.isna(parameters['gpr']) and '>' in str(parameters['gpr']):
            self.create_gpr_triples(control_plan,
                                    Literal(str(parameters['gpr'].replace('>', '')), datatype=XSD.double),
                                    GFAOntoManager.MINIMUM)
        if not pd.isna(parameters['gpr']) and '>' not in str(parameters['gpr']):
            self.create_gpr_triples(control_plan,
                                    Literal(str(parameters['gpr']), datatype=XSD.double))
        if not pd.isna(parameters['max_gfa']):
            self.create_gfa_triples(control_plan,
                                    Literal(str(parameters['max_gfa']), datatype=XSD.double))
        if len(road_buffer_uris) > 0:
            for i in road_buffer_uris:
                self.dataset.add((control_plan, GFAOntoManager.IS_CONSTRAINED_BY, i,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_road_category_triples(self,
                                     road_buffer_dict,
                                     storeys=None,
                                     function=GFAOntoManager.MAXIMUM):
        """
        Generates necessary triples to represent Road Category regulation triples.

        :param road_buffer_dict: List of dictionaries with 'category' and 'buffer' keys.
        :param storeys: Optional number of storeys associated with the road category.
        :param function: Aggregate function URI for storey aggregates.
        :return: List of created road category URIs.
        """

        category_uris = []
        for i in road_buffer_dict:
            category = URIRef(GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH + str(uuid.uuid1()))
            category_uris.append(category)
            buffer = URIRef(GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH + str(uuid.uuid1()))
            measure = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
            self.dataset.add((category, RDF.type, i['category'],
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            self.dataset.add((category, GFAOntoManager.REQUIRES_ROAD_BUFFER, buffer,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            self.dataset.add((buffer, RDF.type, GFAOntoManager.ROAD_BUFFER,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            self.dataset.add((buffer, GFAOntoManager.HAS_VALUE, measure,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            self.dataset.add((measure, GFAOntoManager.HAS_UNIT, GFAOntoManager.METRE,
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            self.dataset.add((measure, GFAOntoManager.HAS_NUMERIC_VALUE, Literal(str(i['buffer']), datatype=XSD.double),
                              GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
            if not pd.isna(storeys):
                storey_aggregate = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
                self.dataset.add((storey_aggregate, RDF.type, GFAOntoManager.STOREY_AGGREGATE,
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))
                self.create_storey_aggregate_triples(category, storey_aggregate, Literal(str(storeys)), function)
        return category_uris

    def create_allowed_gfa_triples(self, obj_uri, gfa_dict):
        """
        Generates necessary triples to represent Gross Floor Area and adds them to the triple dataset.
        :param obj_uri: URI of the city object.
        :param gfa_dict: dictionary with Gross Floor Area values for different programmes.
        """

        obj_uri = URIRef(obj_uri)

        for programme, gfa_value in gfa_dict.items():

            buildable_space_uri = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
            gfa_uri = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))

            measure = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
            gfa_value = Literal(str(gfa_value), datatype=XSD.decimal)

            self.dataset.add((obj_uri, GFAOntoManager.HAS_BUILDABLE_SPACE, buildable_space_uri,
                              GFAOntoManager.BUILDABLE_SPACE_GRAPH))
            self.dataset.add((buildable_space_uri, RDF.type, GFAOntoManager.BUILDABLE_SPACE,
                              GFAOntoManager.BUILDABLE_SPACE_GRAPH))
            self.dataset.add((buildable_space_uri, GFAOntoManager.HAS_ALLOWED_GFA, gfa_uri,
                              GFAOntoManager.BUILDABLE_SPACE_GRAPH))
            self.dataset.add((gfa_uri, RDF.type, GFAOntoManager.GROSS_FLOOR_AREA,
                              GFAOntoManager.BUILDABLE_SPACE_GRAPH))
            self.dataset.add((gfa_uri, GFAOntoManager.HAS_VALUE, measure,
                              GFAOntoManager.BUILDABLE_SPACE_GRAPH))
            self.dataset.add((measure, RDF.type, GFAOntoManager.MEASURE,
                              GFAOntoManager.BUILDABLE_SPACE_GRAPH))
            self.dataset.add((measure, GFAOntoManager.HAS_UNIT, GFAOntoManager.SQUARE_PREFIXED_METRE,
                              GFAOntoManager.BUILDABLE_SPACE_GRAPH))
            self.dataset.add((GFAOntoManager.SQUARE_PREFIXED_METRE, RDF.type, GFAOntoManager.AREA_UNIT,
                              GFAOntoManager.BUILDABLE_SPACE_GRAPH))
            self.dataset.add((measure, GFAOntoManager.HAS_NUMERIC_VALUE, gfa_value,
                              GFAOntoManager.BUILDABLE_SPACE_GRAPH))

            if not pd.isnull(programme):
                programme_uri = URIRef(GFAOntoManager.ONTO_ZONING_URI_PREFIX + programme)
                self.dataset.add((buildable_space_uri, GFAOntoManager.FOR_ZONING_CASE, programme_uri,
                                  GFAOntoManager.BUILDABLE_SPACE_GRAPH))

    def create_site_area_triples(self, plots):
        """
        Generates necessary RDF triples to represent the site area of plots.

        :param plots: A DataFrame containing plot data, including plot URIs and area values.
        """

        for i in plots.index:
            city_obj = URIRef(plots.loc[i, 'plots'])
            if not pd.isna(plots.loc[i, 'plot_area']):
                area_value = Literal(str(plots.loc[i, 'plot_area']), datatype=XSD.decimal)
                site_area = URIRef(URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1())))
                measure = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
                self.dataset.add((site_area, RDF.type, GFAOntoManager.SITE_AREA,
                                  GFAOntoManager.BUILDABLE_SPACE_GRAPH))
                self.dataset.add((city_obj, GFAOntoManager.HAS_SITE_AREA, site_area,
                                  GFAOntoManager.BUILDABLE_SPACE_GRAPH))
                self.dataset.add((site_area, GFAOntoManager.HAS_VALUE, measure,
                                  GFAOntoManager.BUILDABLE_SPACE_GRAPH))
                self.dataset.add((site_area, GFAOntoManager.HAS_UNIT, GFAOntoManager.SQUARE_PREFIXED_METRE,
                                  GFAOntoManager.BUILDABLE_SPACE_GRAPH))
                self.dataset.add((measure, GFAOntoManager.HAS_NUMERIC_VALUE, area_value,
                                  GFAOntoManager.BUILDABLE_SPACE_GRAPH))

    def create_average_width_triples(self, city_obj, width_value):
        """
        Generates RDF triples to represent the average width of a plot.

        :param city_obj: The URIRef of the city object (plot).
        :param width_value: The Literal value representing the average width in metres.
        """

        avg_width = URIRef(URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1())))
        measure = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        self.dataset.add((city_obj, GFAOntoManager.HAS_WIDTH, avg_width,
                          GFAOntoManager.BUILDABLE_SPACE_GRAPH))
        self.dataset.add((avg_width, RDF.type, GFAOntoManager.AVERAGE_WIDTH,
                          GFAOntoManager.BUILDABLE_SPACE_GRAPH))
        self.dataset.add((avg_width, GFAOntoManager.HAS_VALUE, measure,
                          GFAOntoManager.BUILDABLE_SPACE_GRAPH))
        self.dataset.add((measure, GFAOntoManager.HAS_UNIT, GFAOntoManager.METRE,
                          GFAOntoManager.BUILDABLE_SPACE_GRAPH))
        self.dataset.add((measure, GFAOntoManager.HAS_NUMERIC_VALUE, width_value,
                          GFAOntoManager.BUILDABLE_SPACE_GRAPH))

    def create_average_depth_triples(self, city_obj, depth_value):
        """
        Generates RDF triples to represent the average depth of a plot.

        :param city_obj: The URIRef of the city object (plot).
        :param depth_value: The Literal value representing the average depth in metres.
        """

        avg_depth = URIRef(URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1())))
        measure = URIRef(GFAOntoManager.BUILDABLE_SPACE_GRAPH + str(uuid.uuid1()))
        self.dataset.add((city_obj, GFAOntoManager.HAS_DEPTH, avg_depth,
                          GFAOntoManager.BUILDABLE_SPACE_GRAPH))
        self.dataset.add((avg_depth, RDF.type, GFAOntoManager.AVERAGE_DEPTH,
                          GFAOntoManager.BUILDABLE_SPACE_GRAPH))
        self.dataset.add((avg_depth, GFAOntoManager.HAS_VALUE, measure,
                          GFAOntoManager.BUILDABLE_SPACE_GRAPH))
        self.dataset.add((measure, GFAOntoManager.HAS_UNIT, GFAOntoManager.METRE,
                          GFAOntoManager.BUILDABLE_SPACE_GRAPH))
        self.dataset.add((measure, GFAOntoManager.HAS_NUMERIC_VALUE, depth_value,
                          GFAOntoManager.BUILDABLE_SPACE_GRAPH))

    def create_residential_plot_property_triples(self, res_plots, plots):
        """
        Generates necessary triples to represent residential plot properties.

        :param res_plots: GeoDataFrame containing residential plot data.
        :param plots: GeoDataFrame containing general plot data.
        """

        res_plots_df = set_residential_plot_properties(res_plots, plots)
        for i in res_plots_df.index:
            city_obj = URIRef(res_plots_df.loc[i, 'plots'])
            if not pd.isna(res_plots_df.loc[i, 'fringe']):
                at_fringe = Literal(str(res_plots_df.loc[i, 'fringe']), datatype=XSD.boolean)
                self.dataset.add((city_obj, GFAOntoManager.IS_AT_RESIDENTIAL_FRINGE, at_fringe,
                                  GFAOntoManager.BUILDABLE_SPACE_GRAPH))
            if not pd.isna(res_plots_df.loc[i, 'average_width']):
                width_value = Literal(str(res_plots_df.loc[i, 'average_width']), datatype=XSD.decimal)
                self.create_average_width_triples(city_obj, width_value)
            if not pd.isna(res_plots_df.loc[i, 'average_depth']):
                depth_value = Literal(str(res_plots_df.loc[i, 'average_depth']), datatype=XSD.decimal)
                self.create_average_depth_triples(city_obj, depth_value)
            if not pd.isna(res_plots_df.loc[i, 'is_corner_plot']):
                corner_plot = Literal(str(res_plots_df.loc[i, 'is_corner_plot']), datatype=XSD.boolean)
                self.dataset.add((city_obj, GFAOntoManager.IS_CORNER_PLOT, corner_plot,
                                  GFAOntoManager.BUILDABLE_SPACE_GRAPH))

    def create_road_plot_property_triples(self, road_network, road_plots):
        """
        Generates necessary triples to represent road plot properties.

        :param road_network: GeoDataFrame containing road network data.
        :param road_plots: GeoDataFrame containing road plot data.
        """
        road_plots = set_road_plot_properties(road_network, road_plots)
        for i in road_plots.index:
            city_obj_uri = URIRef(road_plots.loc[i, 'plots'])
            if not pd.isna(road_plots.loc[i, 'RD_TYP_CD']):
                road_type = Literal(str(road_plots.loc[i, 'RD_TYP_CD']), datatype=XSD.string)
                self.dataset.add((city_obj_uri, GFAOntoManager.HAS_ROAD_TYPE, road_type,
                                  GFAOntoManager.BUILDABLE_SPACE_GRAPH))

    def create_type_regulation_overlap_triples(self, type_regs):
        """
        Generates necessary triples to represent links between type-based regulations and plots.

        :param type_regs: DataFrame containing type-based regulations and the associated plots they apply to.
        """
        for i in type_regs.index:
            for j in type_regs.loc[i, 'applies_to']:
                self.dataset.add((URIRef(type_regs.loc[i, 'reg']), GFAOntoManager.APPLIES_TO, URIRef(j),
                                  GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def create_neighbor_triples(self, neighbor_links):
        """
        Generate and add RDF triples representing neighboring plot relationships to the dataset.

        :param neighbor_links: GeoDataFrame containing neighboring plot relationships.
        """
        for count, i in enumerate(neighbor_links.index):

            plot_uri = URIRef(neighbor_links.loc[i, 'plots'])
            neighbor_uri = URIRef(neighbor_links.loc[i, 'context_plots'])
            if neighbor_links.loc[i, 'plots'] != neighbor_links.loc[i, 'context_plots']:
                self.dataset.add((plot_uri, GFAOntoManager.HAS_NEIGHBOUR, neighbor_uri,
                                  GFAOntoManager.BUILDABLE_SPACE_GRAPH))

    def create_area_regulation_overlap_triples(self, regulation, plot):
        """
        Generates triples to represent the overlap between a regulation and a plot.

        :param regulation: URI of the regulation entity.
        :param plot: URI of the plot entity.
        """

        city_object_uri = URIRef(regulation.replace('genericcityobject', 'cityobject'))
        plot_uri = URIRef(plot)
        self.dataset.add((city_object_uri, GFAOntoManager.APPLIES_TO, plot_uri,
                          GFAOntoManager.ONTO_PLANNING_REGULATIONS_GRAPH))

    def write_triples(self, triple_type, endpoint=None):
        """
        Write the aggregated triple dataset into an N-Quads file.

        :param triple_type: Type of triples (used in the output filename).
        :param endpoint: (Optional) SPARQL Update endpoint URL to upload triples.
        """
        file_path = f"{self.out_dir}\output_{triple_type}.nq"
        with open(file_path, mode="wb") as file:
            file.write(self.dataset.serialize(format='nquads'))

        logger.info(f"Triples written to {file_path}")

        if endpoint:
            sparql = SPARQLWrapper(endpoint)
            sparql.setMethod(POST)

            for graph in self.dataset.contexts():
                graph_uri = graph.identifier
                triples_nt = graph.serialize(format='nt').decode('utf-8').strip()

                triple_lines = triples_nt.splitlines()
                chunk_size = 10000
                for i in range(0, len(triple_lines), chunk_size):
                    chunk = "\n".join(triple_lines[i:i + chunk_size])
                    if graph_uri:
                        update_query = f"""INSERT DATA {{ GRAPH <{graph_uri}> {{ {chunk} }} }}"""

                        sparql.setQuery(update_query)
                        sparql.query()

                    logger.info(f"{i + chunk_size}/{len(triple_lines)} triples uploaded for graph <{graph_uri}>.")
