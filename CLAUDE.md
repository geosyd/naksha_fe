# Naksha FE CLI Tool
## Project Overview and Success Instructions

## Priority 1: Core Functionality and Architecture
- Use Python at "C:\Python27\ArcGIS10.7\python.exe" with ArcPy integration for geospatial processing
- Modular architecture with focused modules in `src/` directory, keeping main.py around 30 lines
- Short *.py file names (3-4 characters) and optimize src folder files to around 600 lines of code
- Two-stage upload system: file upload + JSON data upload with Naksha REST API communication
- Bearer token authentication for secure API integration
- Comprehensive logging system with selective message capture to data/log.txt
- Processes data in 500-record chunks for efficient memory management
- Clean, colorful CLI output with symbol-free messaging and detailed progress tracking

## Priority 2: Geospatial Processing and Data Management
- Creates PROPERTY_PARCEL feature class with ESRI-compliant fields and unique Global IDs
- Reads SurveyUnitCode from data.csv survey_unit_id column and looks up Ward/Block details from codes.csv
- Uses Ward/Block details to obtain block boundary features from WARD_BLOCK feature class in nblocks.gdb
- Creates 100m buffer around block polygons to extract and clip parcels from nparcels.gdb
- Performs field mapping: state_lgd_cd → state_code, dist_lgd_cd → dist_code, etc.
- GlobalID field recreation using soi_uniq_id with GUID datatype after topology operations
- OBJECTID reset functionality using copy-rename approach to start numbering from 1
- Supports multiple commands: codes, stats, prepare, validate, upload, sanitize, clear
- Enhanced clear command with --gdbs and --logs flags for selective cleanup

## Priority 3: Advanced Sanitization and Validation Features
- 11-step advanced polygon sanitization workflow with comprehensive overlap resolution
- Progressive overlap resolution with buffer-erase operations from 1cm to 80cm in 1cm increments
- Enhanced --buffer-erase flag for specific buffer distances with safety warnings for large values
- Hole detection and repair using NULL separator analysis to distinguish from multipart geometries
- True multipart detection using getPart logic instead of isMultipart flag for accurate classification
- Geometry simplification using ArcPy Generalize tool for complex polygon optimization
- Complex geometry identification (isMultipart=True but partCount=1) without treating as errors
- Enhanced overlap detection with three methodologies: [intersect analysis], geom_overlaps, [intersect geometry]
- Comprehensive 5-method overlap detection: ArcPy SelectLayerByLocation, direct overlap, intersection area, containment, boundary touching
- Progressive tolerance resolution: Snap operations (0.001m → 0.01m → 0.1m) and Integrate operations with detailed verification
- Random pair selection with upfront identification and complete pair resolution before moving to next pair
- In-memory workspace usage for temporary operations, avoiding persistent layer creation
- PROPERTY_PARCEL feature class-only operations with data integrity preservation
- All overlaps must be resolved - no acceptable exceptions or infinite loops allowed