#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Core ArcPy operations for geospatial processing and geometry handling
"""

import os
import shutil
try:
    import arcpy
except ImportError:
    arcpy = None

# Import configuration loader
try:
    from src.util import get_config
except ImportError:
    # Fallback if util module not available
    def get_config():
        class DummyConfig:
            def get_wkid(self):
                return 32644
        return DummyConfig()

# Configuration constants (from removed config.py)
API_BASE_URL = "https://nakshauat.dolr.gov.in"
CHUNK_SIZE = 500
MAX_FEATURES = 1000
BUFFER_DISTANCE = 100
GEOMETRY_TYPE_POLYGON = "POLYGON"
RESPONSE_SUCCESS_CODE = "S-00"
API_TIMEOUT = 30
FILE_OPERATION_TIMEOUT = 60

# Dynamic spatial reference from configuration
def get_spatial_reference():
    """Get spatial reference WKID from configuration"""
    config = get_config()
    return config.get_wkid()

# Field names and mapping
WARD_FIELDS = ['WARD', 'Ward', 'ward', 'WARD_NAME', 'WardName']
BLOCK_FIELDS = ['BLOCK', 'Block', 'block', 'BLOCK_NAME', 'BlockName', 'BLOCK_NO']

# Required fields for property parcels
REQUIRED_FIELDS = [
    'state_lgd_cd', 'dist_lgd_cd', 'ulb_lgd_cd', 'ward_lgd_cd',
    'vill_lgd_cd', 'col_lgd_cd', 'survey_unit_id'
]

# Simple console output functions (consolidated from cons.py)
def print_error(msg):
    print("ERROR: {}".format(msg))

def print_verbose_info(msg, verbose=False):
    if verbose:
        print("INFO: {}".format(msg))

def print_essential_info(msg, verbose=False):
    print(msg)

def print_essential_success(msg):
    print("SUCCESS: {}".format(msg))


class ArcCore:
    """Core ArcPy operations for geospatial processing"""

    @staticmethod
    def is_available():
        """Check if ArcPy is available"""
        return arcpy is not None

    @staticmethod
    def get_spatial_reference(gdb_path, feature_class=None, wkid=None):
        """Get spatial reference from GDB or use default"""
        if not arcpy:
            return None

        try:
            if feature_class:
                fc_path = os.path.join(gdb_path, feature_class)
                if arcpy.Exists(fc_path):
                    desc = arcpy.Describe(fc_path)
                    if desc.spatialReference:
                        return desc.spatialReference

            arcpy.env.workspace = gdb_path
            feature_classes = arcpy.ListFeatureClasses()
            for fc in feature_classes:
                desc = arcpy.Describe(fc)
                if desc.spatialReference:
                    return desc.spatialReference

            if wkid:
                return arcpy.SpatialReference(wkid)

            # Use dynamic spatial reference from configuration
            return arcpy.SpatialReference(get_spatial_reference())

        except Exception as e:
            print_error("Error getting spatial reference: {}".format(e))
            return arcpy.SpatialReference(get_spatial_reference())

    @staticmethod
    def create_parcel_fields(gdb_path, layer_name="PROPERTY_PARCEL"):
        """Create required fields for property parcel feature class"""
        if not arcpy:
            return False

        try:
            layer_path = os.path.join(gdb_path, layer_name)
            if not arcpy.Exists(layer_path):
                print_error("Layer does not exist: {}".format(layer_path))
                return False

            field_definitions = [
                ("state_lgd_cd", "TEXT", "State Code", 50),
                ("dist_lgd_cd", "TEXT", "District Code", 50),
                ("ulb_lgd_cd", "TEXT", "ULB Code", 50),
                ("ward_lgd_cd", "TEXT", "Ward Code", 50),
                ("vill_lgd_cd", "TEXT", "Village Code", 50),
                ("col_lgd_cd", "TEXT", "Column Code", 50),
                ("survey_unit_id", "TEXT", "Survey Unit ID", 50),
                ("soi_uniq_id", "GUID", "SOI Unique ID"),
                ("geom_type", "TEXT", "Geometry Type", 50),
                ("geom_acre", "DOUBLE", "Area in Acres"),
                ("prop_stat", "SHORT", "Property Status"),
                ("land_use", "SHORT", "Land Use Type"),
                ("prop_type", "SHORT", "Property Type"),
                ("owner_typ", "SHORT", "Owner Type"),
                ("ownership", "SHORT", "Ownership Status"),
                ("owner_det", "TEXT", "Owner Details", 254),
                ("owner_shr", "TEXT", "Owner Share", 254),
                ("owner_cnt", "SHORT", "Owner Count"),
                ("last_updat", "DATE", "Last Updated")
            ]

            existing_fields = [f.name for f in arcpy.ListFields(layer_path)]
            for field_def in field_definitions:
                field_name = field_def[0]
                field_type = field_def[1]
                field_alias = field_def[2]
                field_length = field_def[3] if len(field_def) > 3 and field_type == "TEXT" else None

                if field_name not in existing_fields:
                    arcpy.AddField_management(
                        layer_path,
                        field_name,
                        field_type,
                        field_alias=field_alias,
                        field_length=field_length
                    )

            return True

        except Exception as e:
            print_error("Error creating parcel fields: {}".format(e))
            return False

    @staticmethod
    def convert_to_esri_format(geometry, wkid):
        """Convert geometry to ESRI format with specified spatial reference"""
        if not arcpy or not geometry:
            return None

        try:
            spatial_ref = arcpy.SpatialReference(wkid)
            if geometry.spatialReference.factoryCode != wkid:
                geometry = geometry.projectAs(spatial_ref)
            return geometry.JSON

        except Exception as e:
            print_error("Error converting geometry to ESRI format: {}".format(e))
            return None

    @staticmethod
    def convert_multipolygon_to_single(feature_class):
        """Convert multipolygon features to single polygon features with enhanced strategies"""
        if not arcpy:
            return False

        try:
            has_multipart = False
            multipart_count = 0

            # First, count multipart features
            with arcpy.da.SearchCursor(feature_class, ["SHAPE@", "OID@"]) as cursor:
                for row in cursor:
                    geometry, oid = row
                    if geometry and hasattr(geometry, 'isMultipart') and geometry.isMultipart:
                        has_multipart = True
                        multipart_count += 1

            if not has_multipart:
                print_verbose_info("No multipart features found - conversion not needed", True)
                return True

            print_verbose_info("Found {} multipart features - applying enhanced conversion".format(multipart_count), True)

            # Strategy 1: Try ArcPy MultipartToSinglepart_management
            try:
                arcpy.MultipartToSinglepart_management(feature_class)

                # Verify conversion success
                remaining_multipart = 0
                with arcpy.da.SearchCursor(feature_class, ["SHAPE@", "OID@"]) as cursor:
                    for row in cursor:
                        geometry, oid = row
                        if geometry and hasattr(geometry, 'isMultipart') and geometry.isMultipart:
                            remaining_multipart += 1

                if remaining_multipart == 0:
                    print_verbose_info("Strategy 1 successful - All multipart features converted", True)
                    return True
                else:
                    print_verbose_info("Strategy 1 partially successful - {} multipart features remaining".format(remaining_multipart), True)

            except Exception as e:
                print_verbose_info("Strategy 1 (ArcPy MultipartToSinglepart) failed: {}".format(e), True)

            # Strategy 2: Manual conversion for remaining multipart features
            return ArcCore._manual_multipolygon_conversion(feature_class)

        except Exception as e:
            print_error("Error in multipart conversion process: {}".format(e))
            return False

    @staticmethod
    def _manual_multipolygon_conversion(feature_class):
        """Manual conversion of multipart features to single polygons"""
        try:
            import tempfile
            import os

            # Create temporary feature class for manual conversion
            temp_gdb = os.path.join(tempfile.gettempdir(), "temp_manual_conversion.gdb")
            if arcpy.Exists(temp_gdb):
                arcpy.Delete_management(temp_gdb)

            arcpy.CreateFileGDB_management(os.path.dirname(temp_gdb), os.path.basename(temp_gdb))
            temp_fc = os.path.join(temp_gdb, "manual_conversion")

            # Create temp feature class with same structure
            arcpy.CreateFeatureclass_management(
                temp_gdb, "manual_conversion", "POLYGON",
                spatial_reference=arcpy.Describe(feature_class).spatialReference
            )

            # Add all fields from original feature class to temporary feature class
            original_fields = arcpy.ListFields(feature_class)
            for field in original_fields:
                if field.name not in ["OID@", "SHAPE@"]:
                    try:
                        arcpy.AddField_management(temp_fc, field.name, field.type,
                                                field_precision=getattr(field, 'precision', None),
                                                field_scale=getattr(field, 'scale', None),
                                                field_length=getattr(field, 'length', None),
                                                field_alias=field.aliasName,
                                                field_is_nullable=field.isNullable)
                    except:
                        # Field might already exist or have issues, continue
                        pass

            converted_count = 0
            failed_count = 0

            # Process each feature
            with arcpy.da.SearchCursor(feature_class, ["OID@", "SHAPE@"] + [f.name for f in original_fields if f.name not in ["OID@", "SHAPE@"]]) as search_cursor:
                with arcpy.da.InsertCursor(temp_fc, ["SHAPE@"] + [f.name for f in original_fields if f.name not in ["OID@", "SHAPE@"]]) as insert_cursor:

                    for row in search_cursor:
                        oid = row[0]
                        geometry = row[1]
                        attributes = row[2:]

                        if not geometry:
                            failed_count += 1
                            continue

                        # Check if multipart
                        if hasattr(geometry, 'isMultipart') and geometry.isMultipart:
                            # Extract largest part
                            largest_geometry = ArcCore._extract_largest_part(geometry)
                            if largest_geometry:
                                insert_cursor.insertRow([largest_geometry] + list(attributes))
                                converted_count += 1
                            else:
                                failed_count += 1
                        else:
                            # Already single polygon, copy as-is
                            insert_cursor.insertRow([geometry] + list(attributes))

            # Replace original with converted
            if failed_count == 0:
                arcpy.Delete_management(feature_class)
                arcpy.Rename_management(temp_fc, os.path.basename(feature_class))
                # Clean up temp GDB with retry mechanism
                for attempt in range(3):
                    try:
                        arcpy.Delete_management(temp_gdb)
                        break
                    except:
                        import time
                        time.sleep(0.5)  # Wait 500ms and retry
                print_verbose_info("Manual conversion successful - converted {} features".format(converted_count), True)
                return True
            else:
                # Partial success - replace and warn
                arcpy.Delete_management(feature_class)
                arcpy.Rename_management(temp_fc, os.path.basename(feature_class))
                # Clean up temp GDB with retry mechanism
                for attempt in range(3):
                    try:
                        arcpy.Delete_management(temp_gdb)
                        break
                    except:
                        import time
                        time.sleep(0.5)  # Wait 500ms and retry
                print_verbose_info("Manual conversion partially successful - converted {} features, {} failed".format(converted_count, failed_count), True)
                return True

        except Exception as e:
            print_error("Manual multipart conversion failed: {}".format(e))
            return False

    @staticmethod
    def _extract_largest_part(geometry):
        """Extract the largest part from a multipart geometry"""
        try:
            if not hasattr(geometry, 'getPart'):
                return None

            largest_part = None
            largest_area = 0

            part_count = geometry.partCount
            for part_index in range(part_count):
                try:
                    part_array = geometry.getPart(part_index)
                    if part_array and part_array.count > 0:
                        part_polygon = arcpy.Polygon(part_array, geometry.spatialReference)
                        if hasattr(part_polygon, 'area') and part_polygon.area > largest_area:
                            largest_area = part_polygon.area
                            largest_part = part_polygon
                except:
                    continue

            return largest_part

        except Exception as e:
            print_error("Error extracting largest part: {}".format(e))
            return None

    @staticmethod
    def create_buffer(geometry, distance):
        """Create buffer around geometry"""
        if not arcpy or not geometry:
            return None

        try:
            return geometry.buffer(distance)
        except Exception as e:
            print_error("Error creating buffer: {}".format(e))
            return None

    @staticmethod
    def clip_parcels_to_buffer(parcels_layer, buffer_geometry, output_gdb, output_name):
        """Clip parcels to buffer geometry"""
        if not arcpy:
            return None

        try:
            output_path = os.path.join(output_gdb, output_name)
            temp_buffer = os.path.join(output_gdb, "temp_buffer")
            arcpy.CopyFeatures_management([buffer_geometry], temp_buffer)
            arcpy.Clip_analysis(parcels_layer, temp_buffer, output_path)
            arcpy.Delete_management(temp_buffer)
            return output_path

        except Exception as e:
            print_error("Error clipping parcels: {}".format(e))
            return None

    @staticmethod
    def get_feature_classes_in_gdb(gdb_path):
        """Get list of feature classes in geodatabase"""
        if not arcpy:
            return []

        try:
            arcpy.env.workspace = gdb_path
            return arcpy.ListFeatureClasses()
        except Exception as e:
            print_error("Error listing feature classes: {}".format(e))
            return []

    @staticmethod
    def find_feature_class_by_name(gdb_path, name_patterns):
        """Find feature class by name patterns"""
        if not arcpy:
            return None

        try:
            feature_classes = ArcCore.get_feature_classes_in_gdb(gdb_path)

            for pattern in name_patterns:
                if pattern in feature_classes:
                    return pattern

            for fc in feature_classes:
                for pattern in name_patterns:
                    if pattern.lower() in fc.lower():
                        return fc

            return None

        except Exception as e:
            print_error("Error finding feature class: {}".format(e))
            return None

    @staticmethod
    def copy_features_with_projection(input_features, output_path, target_sr):
        """Copy features to new location with projection"""
        if not arcpy:
            return False

        try:
            arcpy.Project_management(input_features, output_path, target_sr)
            return True
        except Exception as e:
            print_error("Error projecting features: {}".format(e))
            return False

    @staticmethod
    def create_gdb(folder_path, gdb_name):
        """Create file geodatabase"""
        if not arcpy:
            return False

        try:
            # Ensure folder exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
            
            # Handle .gdb extension
            if not gdb_name.endswith('.gdb'):
                gdb_name = gdb_name + '.gdb'
            
            gdb_path = os.path.join(folder_path, gdb_name)
            if os.path.exists(gdb_path):
                shutil.rmtree(gdb_path)
            
            # Create GDB using folder name without .gdb extension
            gdb_folder_name = gdb_name[:-4] if gdb_name.endswith('.gdb') else gdb_name
            arcpy.CreateFileGDB_management(folder_path, gdb_folder_name)
            return gdb_path

        except Exception as e:
            print_error("Error creating GDB: {}".format(e))
            return None

    @staticmethod
    def create_feature_class(gdb_path, fc_name, geometry_type="POLYGON", spatial_ref=None):
        """Create feature class"""
        if not arcpy:
            return None

        try:
            # Ensure GDB exists
            if not os.path.exists(gdb_path):
                print_error("GDB does not exist: {}".format(gdb_path))
                return None
            
            # Set workspace to GDB
            arcpy.env.workspace = gdb_path
            
            fc_path = os.path.join(gdb_path, fc_name)
            arcpy.CreateFeatureclass_management(
                gdb_path, fc_name, geometry_type, None, "DISABLED", "DISABLED",
                spatial_ref or arcpy.SpatialReference(get_spatial_reference())
            )
            return fc_path

        except Exception as e:
            print_error("Error creating feature class: {}".format(e))
            return None

    @staticmethod
    def validate_gdb(gdb_path):
        """Validate geodatabase"""
        if not arcpy:
            return False

        try:
            if not os.path.exists(gdb_path):
                return False
            if not gdb_path.endswith('.gdb'):
                return False
            if not os.path.isdir(gdb_path):
                return False
            return True

        except Exception as e:
            print_error("Error validating GDB: {}".format(e))
            return False

    @staticmethod
    def set_arcpy_environment(workspace=None, overwrite=True):
        """Set ArcPy environment settings"""
        if not arcpy:
            return False

        try:
            if workspace:
                arcpy.env.workspace = workspace
            arcpy.env.overwriteOutput = overwrite
            return True

        except Exception as e:
            print_error("Error setting ArcPy environment: {}".format(e))
            return False