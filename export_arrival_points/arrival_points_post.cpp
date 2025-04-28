#include <maps/wikimap/mapspro/cfg/editor/lib/constants.h>
#include <maps/wikimap/mapspro/libs/common/include/default_config.h>
#include <maps/wikimap/mapspro/libs/common/include/extended_xml_doc.h>
#include <maps/wikimap/mapspro/libs/common/include/pgpool3_helpers.h>
#include <maps/wikimap/mapspro/libs/common/include/yt.h>
#include <maps/wikimap/mapspro/libs/configs_editor/include/attrdef.h>
#include <maps/wikimap/mapspro/libs/configs_editor/include/categories.h>
#include <maps/wikimap/mapspro/libs/configs_editor/include/config_holder.h>
#include <maps/wikimap/mapspro/libs/configs_editor/include/externals.h>
#include <maps/wikimap/mapspro/libs/editor_client/include/exception.h>
#include <maps/wikimap/mapspro/libs/editor_client/include/object_helpers.h>
#include <maps/wikimap/mapspro/services/tasks_sprav/src/libs/common/include/name_utils.h>
#include <maps/wikimap/mapspro/services/tasks_sprav/src/libs/common/include/sprav_export.h>
#include <maps/wikimap/mapspro/services/tasks_sprav/src/libs/indoor_levels/include/indoor_level.h>
#include <maps/wikimap/mapspro/services/tasks_sprav/src/merge_poi_worker/names.h>

#include <boost/algorithm/string.hpp>
#include <maps/libs/chrono/include/format.h>
#include <maps/libs/cmdline/include/cmdline.h>
#include <maps/libs/common/include/base64.h>
#include <maps/libs/geolib/include/distance.h>
#include <maps/libs/json/include/builder.h>
#include <maps/libs/log8/include/log8.h>
#include <maps/libs/json/include/prettify.h>
#include <maps/libs/json/include/std.h>
#include <maps/libs/json/include/value.h>
#include <maps/sprav/lib/altay/company/converters/company.h>
#include <sprav/protos/company.pb.h>

const std::string INDOOR_POI_CATEGORY_PREFIX = "indoor_poi_";
const TString CLUSTER = "hahn";
const std::string NYAK_MAPPING_PATH = "//home/maps/users/necromastery/arrival_points/mapping";
const std::string COMPANIES_SPRAV_PATH = "//home/maps/users/necromastery/arrival_points/companies_with_arrival_points";
const std::string ARIVAL_POINTS_JSON_PATH = "arrival_points_input";
const std::string EDITOR_URL_XPATH = "/config/services/editor/url";
const std::string CATEGORIES_CONFIG_XPATH = "/config/services/editor/config";
const std::vector<maps::chrono::TimePoint> DELETE_TIME_BEGIN = {
    maps::chrono::parseIsoDateTime("2025-02-13T00:00:00"),
    maps::chrono::parseIsoDateTime("2025-02-21T00:00:00"),
    maps::chrono::parseIsoDateTime("2025-02-24T00:00:00"),
    maps::chrono::parseIsoDateTime("2025-02-27T19:00:00"),
    maps::chrono::parseIsoDateTime("2025-03-04T15:00:00"),
    maps::chrono::parseIsoDateTime("2025-03-05T02:00:00"),
    maps::chrono::parseIsoDateTime("2025-03-05T15:00:00")};
const std::vector<maps::chrono::TimePoint> DELETE_TIME_END = {
    maps::chrono::parseIsoDateTime("2025-02-14T00:00:00"),
    maps::chrono::parseIsoDateTime("2025-02-22T00:00:00"),
    maps::chrono::parseIsoDateTime("2025-02-25T00:00:00"),
    maps::chrono::parseIsoDateTime("2025-02-27T21:00:00"),
    maps::chrono::parseIsoDateTime("2025-03-04T16:00:00"),
    maps::chrono::parseIsoDateTime("2025-03-05T03:00:00"),
    maps::chrono::parseIsoDateTime("2025-03-05T17:00:00")};

const std::unordered_set<maps::wiki::poi_feed::PermalinkId> VALID_FAR_PERMALINKS = {
    56744253706ul,  212395949761ul, 213744556105ul, 149780337092ul, 78151547007ul,  184505070850ul,
    1101237450ul,   1101237450ul,   166353001106ul, 25256762273ul,  165516443005ul, 207094637598ul,
    40425106479ul,  40425106479ul,  40425106479ul,  40425106479ul,  40425106479ul,  19884016044ul,
    176019636919ul, 204097130151ul, 7398041697ul,   7398041697ul,   79140071974ul,  1023173008ul,
    212341146657ul, 39039609800ul,  23921130851ul,  111032732834ul, 230532987724ul, 2661130439ul,
    242463502440ul, 143504819575ul, 1124367034ul,   165516443005ul, 61318757205ul,  161554055900ul,
    162712013315ul, 162776846058ul, 40425106479ul,  97872933064ul,  167049173168ul, 148234605851ul,
    1773626692ul,   164983526021ul, 7398041697ul,  185374913865ul,  1101237450ul,   32626785590ul,
    211974407453ul, 60431390325ul,  1273654235ul,  125716678461ul,  19884016044ul
};
const std::unordered_set<uint64_t> INVALID_IDS = {};
const std::unordered_map<uint64_t, std::vector<std::vector<double>>> VALET_ARRIVAL_POINTS = {
    {4863129576, {{55.197578, 25.118883}}},
    {2452955890, {{32.777618, 39.909905}}},
    {2452958437, {{29.166799, 40.920003}}},
    {2452959834, {{29.12262, 40.952017}}},
    {2452956308, {{37.361691, 37.063408}}},
    {2452956335, {{32.809961, 39.912073}}},
    {2452956120, {{28.95996, 40.249161}}},
    {2452956128, {{28.992735, 41.050703}, {28.993188, 41.051139}}},
    {2452956198, {{28.687312, 41.003061}}},
    {4399712080, {{28.987599, 41.036954}}},
    {5490686007, {{46.70446, 24.956673}}},
    {2452956667, {{32.762512, 39.945788}}},
    {2452957750, {{28.992251, 41.067323}}},
    {2452959611, {{29.013292, 41.077496}}},
    {2452957572, {{28.8066, 41.064342}, {28.807617, 41.06452}}},
    {2452957390, {{32.810392, 39.888269}}},
    {2452959814, {{29.945945, 40.756273}}},
    {2452959145, {{29.97896, 40.759382}, {29.97879, 40.757677}}},
    {4064702095, {{29.123327, 40.993541}, {29.124344, 40.993783}}},
    {2452956764, {{29.099782, 40.984714}, {29.100662, 40.985161}}},
    {2452956784, {{28.870804, 40.977899}, {28.871491, 40.976935}}},
    {2452956833, {{29.040652, 41.020845}}},
    {5640269767, {{46.643111, 24.726426}}},
    {3974914020, {{27.068202, 38.398201}}},
    {1542631709, {{55.241268, 25.169902}}},
    {1604756739, {{55.351622, 25.248753}}},
    {1542628291, {{55.199998, 25.119535}}},
    {2452962651, {{29.179191, 41.015266}}},
    {2452958315, {{29.018274, 41.067037}}},
    {2452962172, {{28.996239, 40.207978}}},
    {4840228766, {{55.12204, 25.079825}, {55.123258, 25.078506}}},
    {2196214745, {{29.315765, 40.908469}}},
    {2100403461, {{55.353057, 25.219619}}},
    {4199250383, {{32.827464, 39.84519}}},
    {3634789974, {{55.240217, 25.103062}, {55.241311, 25.102295}}},
    {4851891196, {{55.421898, 25.173404}}},
    {4609579273, {{55.275383, 25.199381}, {55.279528, 25.199022}}},
    {5733345497, {{46.711813, 24.940717}}},
    {2115236229, {{55.409542, 25.217109}, {55.409622, 25.217542}}},
    {2452959908, {{27.173511, 38.44718}}},
    {3238294597, {{28.740834, 41.258401}, {28.743849, 41.25848}}},
    {2452957731, {{28.668508, 41.05621}}},
    {2452957734, {{37.319075, 37.051136}}},
    {4601981962, {{55.281174, 25.212988}}},
    {4720824473, {{55.122332, 25.126512}}},
    {3232362057, {{54.391095, 24.499397}}},
    {4166373709, {{27.052169, 38.49028}}},
    {2452957016, {{29.006068, 41.068148}}},
    {1542615687, {{55.289124, 25.226096}, {55.289548, 25.223494}}},
    {5490685767, {{46.70439, 24.956768}}},
    {2452957503, {{28.797972, 40.965832}, {28.800724, 40.964271}}},
    {2452961154, {{37.383094, 37.065228}}},
    {2452958504, {{28.993108, 41.06392}, {28.993637, 41.061289}}},
    {2819509154, {{31.003891, 36.88021}}},
    {2452957609, {{29.071201, 41.002301}, {29.071896, 41.002471}}},
    {3108474381, {{27.179816, 38.480827}}},
    {1604756529, {{55.351622, 25.248764}, {55.355356, 25.267819}}},
    {2452959738, {{35.305689, 36.993489}, {35.308876, 36.993335}}},
    {2452957800, {{28.999238, 41.055568}}},
    {4438928510, {{28.986052, 41.028486}, {28.987184, 41.028783}}},
    {1542615786, {{54.368653, 24.500209}}},
    {2452955777, {{28.850655, 40.977748}, {28.850666, 40.978744}}},
    {4102377602, {{32.854334, 39.904479}}},
    {2452955849, {{27.12958, 38.421835}}},
    {2452967573, {{28.985628, 41.104807}, {28.986413, 41.108847}, {28.986485, 41.107646}}},
    {2452962081, {{32.609886, 39.983648}}},
    {3731793336, {{27.0749, 38.476612}, {27.075324, 38.477757}, {27.074565, 38.480034}}},
    {4931607526, {{55.279226, 25.210116}}},
    {2452955293, {{35.24259, 37.01637}}},
    {2452959822, {{32.83231, 39.950653}}},
    {2376820391, {{55.122438, 25.079939}, {55.123366, 25.078908}}},
    {4832325386, {{55.122098, 25.079829}, {55.123287, 25.078576}}},
    {1604757499, {{55.355242, 25.267884}}}
};
const std::unordered_set<std::string> INVALID_CATEGORY_IDS = {
    "transport_metro_station", "transport_railway_station",
    "transport_terminal", "transport_airport_terminal",
    "transport_airport", "transport_waterway_stop",
    "transport_stop"
};
const std::unordered_set<std::string> VALID_CATEGORY_IDS = {
    "poi_construction", "poi_industry", "poi_service", "poi_urban", "poi_leisure",
    "poi_culture", "poi_sport",  "poi_auto", "poi_food",  "poi_religion", "poi_goverment",
    "poi_shopping", "poi_finance",  "poi_edu", "poi_medicine", "transport_waterway_stop",
    "transport_terminal", "transport_metro_station",  "transport_metro_exit",
    "transport_railway_station", "hydro_point", "vegetation", "relief_point",
    "transport_airport_terminal", "transport_airport", "transport_stop"
};

struct NyakMapping {
    uint64_t dbid;
    std::string publishingStatus;

    NyakMapping() = default;
    NyakMapping(uint64_t dbid, const std::string& publishingStatus) : dbid(dbid), publishingStatus(publishingStatus) {};
};

struct CompanySprav {
    NSpravTDS::Company company;
    maps::wiki::poi_feed::PermalinkId permalink;
};

struct ArrivalPoint {
    double findDistanceToAssigned() {
        double distance = 0;
        for (const auto& masterOrg : masterOrgs) {
            try {
                distance = std::max(
                    distance,
                    maps::geolib3::geoDistance(geo, masterOrg.getGeometryInGeodetic().value().get<maps::geolib3::Point2>()));
            } catch (const std::exception& e) {
                INFO() << "ID: " << id << " MASTER ORG GEO IS NOT POINT";
            }
        }
        return distance;
    }

    // maps::wiki::editor_client::BasicEditorObject
    // findIndoorPoiMaster(
    //     const maps::wiki::editor_client::BasicEditorObject& indoorPoi,
    //     const maps::wiki::editor_client::Instance& editor)
    // {
    //     const auto& indoorLevel = editor.getObjectById(indoorPoi.mastersByRole().at("assigned_" + indoorPoi.categoryId).front().id);
    //     const auto& indoorPlan = editor.getObjectById(indoorLevel.mastersByRole().at("assigned").front().id);
    //     return editor.getObjectById(indoorPlan.mastersByRole().at("indoor_plan_assigned").front().id);
    // }

    void findOrgs(
        const maps::json::Value& masterOrgsJson,
        const maps::wiki::editor_client::Instance& editor)
    {
        std::unordered_set<uint64_t> existingMasterOrgs;
        for (const auto& masterOrg : masterOrgsJson) {
            try {
                auto obj = editor.getObjectById(masterOrg["id"].as<uint64_t>());
                if (obj.deleted()) {
                    INFO() << "DELETED. ID: " << obj.id;
                    continue;
                }
                if (obj.categoryId.starts_with(INDOOR_POI_CATEGORY_PREFIX)) {
                    // obj = findIndoorPoiMaster(obj, editor);
                    INFO() << "INDOOR. ID: " << obj.id;
                    continue;
                }
                if (!VALID_CATEGORY_IDS.contains(obj.categoryId)) {
                    INFO() << "UNSUPPORTED CATEGORY. ID: " << obj.id;
                    continue;
                }
                if (existingMasterOrgs.contains(obj.id)) {
                    INFO() << "EXISTING. ID: " << obj.id;
                    continue;
                }
                masterOrgs.push_back(obj);
                existingMasterOrgs.insert(obj.id);
            } catch (...) {
                INFO() << "MASTER ORG NOT FOUND";
            }
        }
        INFO() << "MASTERS CONUT: " << masterOrgs.size();
        for (const auto& masterOrg : masterOrgs) {
            INFO() << masterOrg.categoryId;
        }
    }

    void findParkings(const maps::wiki::editor_client::Instance& editor) {
        if (!isParking) {
            return;
        }
        const auto& mercator = maps::geolib3::convertGeodeticToMercator(geo);
        const auto& parkingIds = editor.getObjectsByLasso(
            {"urban_roadnet_parking_lot"},
            mercator,
            200,
            100,
            maps::wiki::editor_client::GeomPredicate::Intersects);
        for (const auto& parking : parkingIds) {
            const auto& parkingObj = editor.getObjectById(parking.id);
            if (parkingObj.plainAttributes.at("urban_roadnet_parking_lot:residental") == "1") {
                continue;
            }
            if (parkingObj.deleted()) {
                continue;
            }
            parkingsFound.push_back(parkingObj);
        }
        double minDistance = std::numeric_limits<double>::max();
        for (const auto& parking : parkingsFound) {
            const auto distance = maps::geolib3::distance(
                mercator,
                parking.getGeometryInMercator().value().get<maps::geolib3::Point2>());
            const auto objIsToll = parking.plainAttributes.at("urban_roadnet_parking_lot:toll") == "1";
            const auto objIsBld = parking.plainAttributes.at("urban_roadnet_parking_lot:bld") == "1";
            if (objIsBld == isBld && objIsToll == isToll) {
                if (parkingsFoundGood.empty()) {
                    minDistance = distance;
                    slaveParking = parking;
                } else if (distance < minDistance) {
                    minDistance = distance;
                    slaveParking = parking;
                }
                parkingsFoundGood.push_back(parking);
            } else if (parkingsFoundGood.empty() && distance < minDistance) {
                minDistance = distance;
                slaveParking = parking;
            }
        }
    }

    ArrivalPoint() = default;

    ArrivalPoint(
        const NSpravTDS::DrivingArrivalPoint& arrivalPoint,
        const std::vector<maps::wiki::editor_client::BasicEditorObject>& masterOrgs,
        const maps::wiki::poi_feed::PermalinkId permalink,
        const maps::wiki::editor_client::Instance& editor,
        const size_t id,
        bool parkingFinding = true) :
        permalink(permalink),
        id(id),
        masterOrgs(masterOrgs)
    {
        const auto rating = arrivalPoint.rating();
        isMajor = (rating == 1);
        geo = maps::geolib3::Point2(
            arrivalPoint.anchor().point().lon(),
            arrivalPoint.anchor().point().lat());
        for (const auto& description : arrivalPoint.description()) {
            std::string lang = description.lang().locale();
            descriptions[description.value()] = boost::to_lower_copy(lang);
        }
        for (const auto& tag : arrivalPoint.tags()) {
            tags.push_back(tag);
            if (tag == "parking" || tag == "on street, parking" || tag == "parking, free") {
                isParking = true;
            }
            if (tag == "toll") {
                isParking = true;
                isToll = true;
            }
            if (tag == "building" || tag == "buidling" || tag == "in_building") {
                isParking = true;
                isBld = true;
            }
            if (tag == "drop_off") {
                isDropOff = true;
            }
            if (tag == "flight_arrival") {
                isArrival = true;
            }
            if (tag == "flight_departure") {
                isDeparture = true;
            }
        }
        // isParking = isDropOff ? false : isParking;
        distanceToAssigned = findDistanceToAssigned();
        if (parkingFinding) {
            findParkings(editor);
        }
    }

    ArrivalPoint(
        const maps::json::Value& arrivalPointJson,
        const maps::wiki::editor_client::Instance& editor)
    {
        dbid = arrivalPointJson["dbid"].as<uint64_t>();
        if (dbid != 0) {
            return;
        }
        descriptions = arrivalPointJson["descriptions"].as<std::unordered_map<std::string, std::string>>();
        distanceToAssigned = arrivalPointJson["distanceToAssigned"].as<double>();
        geo = maps::geolib3::Point2(arrivalPointJson["geo"]["lon"].as<double>(), arrivalPointJson["geo"]["lat"].as<double>());
        id = arrivalPointJson["id"].as<uint64_t>();
        INFO() << "ID: " << id;
        isArrival = arrivalPointJson["isArrival"].as<bool>();
        isBadMercator = arrivalPointJson["isBadMercator"].as<bool>();
        isBld = arrivalPointJson["isBld"].as<bool>();
        isDeparture = arrivalPointJson["isDeparture"].as<bool>();
        isDublicate = arrivalPointJson["isDublicate"].as<bool>();
        isMajor = arrivalPointJson["isMajor"].as<bool>();
        tags = arrivalPointJson["tags"].as<std::vector<std::string>>();
        isDropOff = std::find(tags.begin(), tags.end(), "drop_off") != tags.end();
        isValet = std::find(tags.begin(), tags.end(), "valet") != tags.end();
        if (isValet) {
            return;
        }
        isParking = arrivalPointJson["isParking"].as<bool>();
        isToll = arrivalPointJson["isToll"].as<bool>();
        permalink = arrivalPointJson["permalink"].as<maps::wiki::poi_feed::PermalinkId>();

        findOrgs(arrivalPointJson["masterOrgs"], editor);
        if (arrivalPointJson["slaveParking"]["id"].as<uint64_t>() != 0) {
            try {
                slaveParking = editor.getObjectById(arrivalPointJson["slaveParking"]["id"].as<uint64_t>());
            } catch (...) {
                INFO() << "PARKING NOT FOUND";
            }
        }
    }

    maps::json::Value dbObjectJson(const maps::wiki::editor_client::BasicEditorObject& dbObject) const {
        maps::json::Builder builder;
        builder << [&](maps::json::ObjectBuilder objectBuilder) {
            objectBuilder["id"] = dbObject.id;
            objectBuilder["categoryId"] = dbObject.categoryId;
            try {
                const auto& point = dbObject.getGeometryInGeodetic().value().get<maps::geolib3::Point2>();
                objectBuilder["lon"] = point.x();
                objectBuilder["lat"] = point.y();
            } catch (std::exception& e) {
                INFO() << "DB object with id " << dbObject.id << " is not point";
            }
        };
        return maps::json::Value::fromString(builder.str());
    }

    maps::json::Value toJson() const {
        maps::json::Builder builder;
        builder << [&](maps::json::ObjectBuilder point) {
            point["permalink"] = permalink;
            point["id"] = id;
            point["dbid"] = dbid;
            point["isParking"] = isParking;
            point["isBld"] = isBld;
            point["isToll"] = isToll;
            point["isArrival"] = isArrival;
            point["isDeparture"] = isDeparture;
            point["isMajor"] = isMajor;
            point["isDublicate"] = isDublicate;
            point["isDropOff"] = isDropOff;
            point["isValet"] = isValet;
            point["distanceToAssigned"] = distanceToAssigned;
            point["isBadMercator"] = isBadMercator;
            point["masterOrgs"] << [&](maps::json::ArrayBuilder orgs) {
                for (const auto& org : masterOrgs) {
                    orgs << dbObjectJson(org);
                }
            };
            point["slaveParking"] << dbObjectJson(slaveParking);
            point["parkingsFound"] << [&](maps::json::ArrayBuilder parkings) {
                for (const auto& parking : parkingsFound) {
                    parkings << dbObjectJson(parking);
                }
            };
            point["parkingsFoundGood"] << [&](maps::json::ArrayBuilder parkings) {
                for (const auto& parking : parkingsFoundGood) {
                    parkings << dbObjectJson(parking);
                }
            };
            point["geo"] << [&](maps::json::ObjectBuilder geoBuilder) {
                geoBuilder["lon"] = geo.x();
                geoBuilder["lat"] = geo.y();
            };
            point["descriptions"] << descriptions;
            point["tags"] << [&](maps::json::ArrayBuilder tagsBuilder) {
                tagsBuilder.putRange(tags);
            };
        };
        return maps::json::Value::fromString(builder.str());
    }

    bool isValid() const {
        if (isValet) {
            INFO() << "VALET. ID: " << id;
            return false;
        }
        if (masterOrgs.empty()) {
            INFO() << "masterOrgs is NULL. ID: " << id;
            return false;
        }
        size_t airpostCount = 0;
        for (const auto& masterOrg : masterOrgs) {
            if (masterOrg.categoryId == "transport_airport" || masterOrg.categoryId == "transport_airport_terminal") {
                airpostCount++;
            }
        }
        if (airpostCount > 0 && airpostCount < masterOrgs.size()) {
            INFO() << "airports error. ID " << id;
            return false;
        }
        if (!VALID_FAR_PERMALINKS.contains(permalink) && distanceToAssigned > 1000.0) {
            INFO() << "masterOrgs far. ID: " << id;
            return false;
        }
        if (INVALID_IDS.contains(id)) {
            INFO() << "INVALID_IDS. ID: " << id;
            return false;
        }
        return true;
    }

    std::optional<maps::wiki::editor_client::BasicEditorObject>
    toEditorObject(
        const maps::wiki::configs::editor::ConfigHolder& editorCfg,
        const std::unordered_set<std::string>& langs) const
    {
        if (!isValid()) {
            return std::nullopt;
        }

        auto category = maps::wiki::configs::editor::CATEGORY_ARRIVAL_POINT;
        if (masterOrgs[0].categoryId == "transport_airport" || masterOrgs[0].categoryId == "transport_airport_terminal") {
            category = maps::wiki::configs::editor::CATEGORY_ARRIVAL_POINT_AIRPORT;
        }
        auto newObject = maps::wiki::editor_client::createWithDefaultAttributes(
            category,
            editorCfg);
        newObject.setGeometryInGeodetic(geo);

        for (const auto& [name, lang]: descriptions) {
            if (!langs.contains(lang)) {
                INFO() << "UNSUPPORTED LANG: " << lang;
                continue;
            }
            newObject.tableAttributes["arrival_point_nm"].push_back(
                {{"arrival_point_nm:lang", lang},
                {"arrival_point_nm:name", name}});
        }
        newObject.plainAttributes["arrival_point:dropoff_pickup"] = boost::lexical_cast<std::string>(isDropOff);
        newObject.plainAttributes["arrival_point:is_major"] = boost::lexical_cast<std::string>(isMajor);
        if (category == maps::wiki::configs::editor::CATEGORY_ARRIVAL_POINT_AIRPORT) {
            newObject.plainAttributes["arrival_point_airport:is_arrival"] = boost::lexical_cast<std::string>(isArrival);
            newObject.plainAttributes["arrival_point_airport:is_departure"] = boost::lexical_cast<std::string>(isDeparture);
        }
        std::vector<maps::wiki::revision::RevisionID> added;
        for (const auto& masterOrg : masterOrgs) {
            added.push_back(masterOrg.revisionId.value());
        }
        newObject.mastersDiff["assigned_arrival_point"] = maps::wiki::editor_client::RelativesDiff(added, {});
        if (slaveParking.id != 0) {
            newObject.slavesDiff["assigned_urban_roadnet_parking_lot"] =
                maps::wiki::editor_client::RelativesDiff({slaveParking.revisionId.value()}, {});
        }

        return newObject;
    }

    maps::geolib3::Point2 geo;
    std::unordered_map<std::string, std::string> descriptions;
    std::vector<std::string> tags;
    maps::wiki::poi_feed::PermalinkId permalink;
    uint64_t id = 0;
    uint64_t dbid = 0;
    bool isParking = false;
    bool isBld = false;
    bool isToll = false;
    bool isDropOff = false;
    bool isValet = false;
    bool isArrival = false;
    bool isDeparture = false;
    bool isMajor = false;
    bool isDublicate = false;
    bool isBadMercator = false;
    double distanceToAssigned = 0;

    std::vector<maps::wiki::editor_client::BasicEditorObject> masterOrgs;
    maps::wiki::editor_client::BasicEditorObject slaveParking;
    std::vector<maps::wiki::editor_client::BasicEditorObject> parkingsFound;
    std::vector<maps::wiki::editor_client::BasicEditorObject> parkingsFoundGood;
};

std::unordered_map<maps::wiki::poi_feed::PermalinkId, std::vector<NyakMapping>>
permalinkToDBID(
    const std::string& path)
{
    INFO() << "Mapping started";

    std::unordered_map<maps::wiki::poi_feed::PermalinkId, std::vector<NyakMapping>> result = { // точки еще непопавшие в таблицу алтая
        {11924246096ul, {NyakMapping(6315174823ul, "publish")}},
        {130521445447ul, {NyakMapping(6315174883ul, "publish")}},
        {135299512009ul, {NyakMapping(6315174993ul, "publish")}},
        {143892640951ul, {NyakMapping(6315174893ul, "publish")}},
        {215774590483ul, {NyakMapping(6315175013ul, "publish")}},
        {63052416745ul, {NyakMapping(6315174933ul, "publish")}},
        {77661305767ul, {NyakMapping(6315174973ul, "publish")}},
        {86529324357ul, {NyakMapping(6315174843ul, "publish")}},
        {132483106067ul, {NyakMapping(6309573810ul, "publish")}}};

    auto client = maps::wiki::common::yt::createYtClient(CLUSTER);
    for (auto reader = client->CreateTableReader<NYT::TNode>(TString(path)); reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        const maps::wiki::poi_feed::PermalinkId permalink = row["permalink"].IntCast<maps::wiki::poi_feed::PermalinkId>();
        const std::string status = row["publishing_status"].AsString();
        const std::string id = row["original_id"].IsNull() ? "0" : row["original_id"].AsString();
        result[permalink].emplace_back(NyakMapping(stoull(id), status));
    }
    result[1066201831ul] = {NyakMapping(1663627781ul, "publish")};
    result[183407255165ul] = {NyakMapping(1572780712ul, "publish")};
    result[1047617319ul] = {NyakMapping(6147307069ul, "publish")};
    result[1100956560ul] = {NyakMapping(1489596980ul, "publish")};
    result[1324876858ul] = {NyakMapping(1603334849ul, "publish")};
    result[172220425702ul] = {NyakMapping(4307192668ul, "publish")};

    INFO() << "Mapping finished";
    return result;
}

std::vector<CompanySprav>
readCompaniesYT(
    const std::string& path)
{
    INFO() << "Arrival points reading started";

    std::vector<CompanySprav> result;
    auto client = maps::wiki::common::yt::createYtClient(CLUSTER);
    for (auto reader = client->CreateTableReader<NYT::TNode>(TString(path)); reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        const auto permalink = row["permalink"].IntCast<maps::wiki::poi_feed::PermalinkId>();
        NSpravTDS::Company company;
        Y_PROTOBUF_SUPPRESS_NODISCARD company.ParseFromString(row["source_proto"].AsString());
        // for (const auto& arrivalPoint : company.driving_arrival_points()) {
        //     for (const auto& tag : arrivalPoint.tags()) {
        //         if (tag == "valet") {
        //             std::cout << permalink << " " <<
        //                 std::to_string(arrivalPoint.anchor().point().lon()) << " " <<
        //                 std::to_string(arrivalPoint.anchor().point().lat()) << std::endl;
        //         }
        //     }
        // }
        result.emplace_back(CompanySprav(company, permalink));
    }

    INFO() << "Arrival points reading finished";
    return result;
}

void
writeArrivalPointsJson(
    const std::unordered_map<maps::wiki::poi_feed::PermalinkId, std::vector<ArrivalPoint>>& companyArrivalPoints,
    const std::string& fileName)
{
    maps::json::Builder builder;
    builder << [&](maps::json::ObjectBuilder companiesBuilder) {
        for (const auto& [permalink, arrivalPoints] : companyArrivalPoints) {
            companiesBuilder[std::to_string(permalink)] << [&](maps::json::ArrayBuilder arrivalPointsBuilder) {
                for (const auto& arrivalPoint : arrivalPoints) {
                    arrivalPointsBuilder << arrivalPoint.toJson();
                }
            };
        }
    };
    std::ofstream outFile(fileName);
    outFile << maps::json::prettifyJson(builder.str()) << std::endl;
    outFile.close();
}

// std::vector<ArrivalPoint>
// readArrivalPointsJson(
//     const std::string& fileName,
//     const std::unordered_map<maps::wiki::poi_feed::PermalinkId, std::vector<NyakMapping>>& nyakMapping,
//     const maps::wiki::editor_client::Instance& editor)
// {
//     const auto& arrivalPointsJson = maps::json::Value::fromFile(fileName);
//     std::vector<ArrivalPoint> arrivalPoints;
//     for (const auto& arrivalPointJson : arrivalPointsJson) {
//         const auto& id = arrivalPointJson["id"].as<uint64_t>();
//         if (!VALID_FAR_PERMALINKS.contains(id)) {
//             continue;
//         }
//         const auto& arrivalPoint = ArrivalPoint(arrivalPointJson, nyakMapping, editor);
//         if (arrivalPoint.dbid != 0) {
//             continue;
//         }
//         INFO() << "Arrival point " << arrivalPoint.id << " READED";
//         arrivalPoints.push_back(arrivalPoint);
//     }
//     return arrivalPoints;
// }

// void
// writeArrivalPointsJson(
//     const std::vector<ArrivalPoint>& arrivalPoints,
//     const std::string& fileName)
// {
//     maps::json::Builder points;
//     points << [&](maps::json::ArrayBuilder points) {
//         for (const auto& arrivalPoint : arrivalPoints) {
//             points << arrivalPoint.toJson();
//         }
//     };
//     std::ofstream outFile(fileName);
//     outFile << maps::json::prettifyJson(points.str()) << std::endl;
//     outFile.close();
// }


// void
// findDublicates(
//     const std::vector<ArrivalPoint>& arrivalPoints,
//     const std::string& fileName)
// {
//     INFO() << "DUBLICATES";
//     std::ofstream outFile(fileName);

//     for (size_t i = 0; i < arrivalPoints.size(); ++i) {
//         if (arrivalPoints[i].dbid == 0) {
//             continue;
//         }
//         for (size_t j = i + 1; j < arrivalPoints.size(); ++j) {
//             if (arrivalPoints[j].dbid == 0) {
//                 continue;
//             }
//             if (maps::geolib3::geoDistance(arrivalPoints[i].geo, arrivalPoints[j].geo) > 2) {
//                 continue;
//             }
//             // arrivalPoints[i].isDublicate = arrivalPoints[j].isDublicate = true;
//             INFO() << arrivalPoints[i].id << ' ' << arrivalPoints[j].id;
//             outFile << arrivalPoints[i].dbid << ' ' << arrivalPoints[j].dbid << std::endl;
//         }
//     }

//     outFile.close();
// }

struct DublicatePair
{
    ArrivalPoint p1;
    ArrivalPoint p2;

    DublicatePair() = default;
    DublicatePair(const ArrivalPoint& p1, const ArrivalPoint& p2) {
        if (p1.id < p2.id) {
            this->p1 = p1;
            this->p2 = p2;
        } else {
            this->p1 = p2;
            this->p2 = p1;
        }
    }

    maps::json::Value toJson() const {
        maps::json::Builder builder;
        builder << [&](maps::json::ObjectBuilder dublicateBuilder) {
            dublicateBuilder["p1"] << [&](maps::json::ObjectBuilder pointBuilder) {
                pointBuilder["permalink"] << p1.permalink;
                pointBuilder["id_in_nyak"] << p1.dbid;
                pointBuilder["lon"] << p1.geo.x();
                pointBuilder["lat"] << p1.geo.y();
            };
            dublicateBuilder["p2"] << [&](maps::json::ObjectBuilder pointBuilder) {
                pointBuilder["permalink"] << p1.permalink;
                pointBuilder["id_in_nyak"] << p1.dbid;
                pointBuilder["lon"] << p1.geo.x();
                pointBuilder["lat"] << p1.geo.y();
            };
        };
        return maps::json::Value::fromString(builder.str());
    }
};

void outputDublicates(
    const std::vector<DublicatePair>& dublicates,
    const std::string& fileName)
{
    maps::json::Builder builder;
    for (const auto& dublicate : dublicates) {
        builder << dublicate.toJson();
    }

    std::ofstream outFile(fileName);
    outFile << maps::json::prettifyJson(builder.str()) << std::endl;
    outFile.close();
}

bool
validateDublicate(const DublicatePair& dublicate, const std::vector<DublicatePair>& dublicates)
{
    for (const auto& existingDublicate : dublicates) {
        if (existingDublicate.p1.id == dublicate.p1.id && existingDublicate.p2.id == dublicate.p2.id) {
            return false;
        }
    }
    return true;
}

void addDublicates(const std::vector<DublicatePair>& dublicates  , std::vector<DublicatePair>& dublicatesResult) {
    for (const auto& dublicate : dublicates) {
        if (validateDublicate(dublicate, dublicatesResult)) {
            dublicatesResult.push_back(dublicate);
        }
    }
}

std::vector<DublicatePair>
findDublicates(
    const std::vector<ArrivalPoint>& arrivalPoints1,
    const std::vector<ArrivalPoint>& arrivalPoints2)
{
    std::vector<DublicatePair> dublicates;
    for (const auto& arrivalPoint1 : arrivalPoints1) {
        for (const auto& arrivalPoint2 : arrivalPoints2) {
            if (arrivalPoint1.dbid == 0 || arrivalPoint2.dbid == 0) {
                continue;
            }
            if (maps::geolib3::geoDistance(arrivalPoint1.geo, arrivalPoint2.geo) > 2) {
                continue;
            }
            INFO() << arrivalPoint1.id << ' ' << arrivalPoint2.id;
            dublicates.push_back(DublicatePair(arrivalPoint1, arrivalPoint2));
        }
    }
    return dublicates;
}

void
findDublicates(
    const std::unordered_map<maps::wiki::poi_feed::PermalinkId, std::vector<ArrivalPoint>>& companyArrivalPoints,
    const std::string& fileName)
{
    INFO() << "Dublicates finding started";

    std::vector<DublicatePair> dublicatesResult;
    for (const auto& [permalink1, arrivalPoints1] : companyArrivalPoints) {
        for (const auto& [permalink2, arrivalPoints2] : companyArrivalPoints) {
            if (permalink1 == permalink2) {
                continue;
            }
            addDublicates(findDublicates(arrivalPoints1, arrivalPoints2), dublicatesResult);
        }
    }

    outputDublicates(dublicatesResult, fileName);

    INFO() << "Dublicates finding finished";
}

// bool validateCompanyArrivalPoints(
//     const std::vector<maps::wiki::editor_client::BasicEditorObject>& companiesNyak,
//     const size_t arrivalPointsSprav
// ) {
//     for (const auto& companyNyak : companiesNyak) {
//         if (INVALID_CATEGORY_IDS.contains(companyNyak.categoryId)) {
//             return false;
//         }
//         size_t arrivalPointsNyak = 0;
//         if (companyNyak.slavesByRole().contains("assigned_arrival_point")) {
//             arrivalPointsNyak = companyNyak.slavesByRole().at("assigned_arrival_point").size();
//         }
//         if (arrivalPointsNyak != arrivalPointsSprav) {
//             return false;
//         }
//     }
//     return true;
// }

bool validateCommitTime(const maps::chrono::TimePoint& commit_time) {
    for (size_t i = 0; i < DELETE_TIME_BEGIN.size(); ++i) {
        if (commit_time >= DELETE_TIME_BEGIN[i] && commit_time <= DELETE_TIME_END[i]) {
            return true;
        }
    }
    INFO() << "TIME: " << maps::chrono::formatIsoDateTime(commit_time);
    return false;
}

void deleteArrivalPoint(
    const uint64_t arrivalPointId,
    maps::wiki::editor_client::Instance& editor,
    int64_t uid,
    bool dryRun)
{
    try {
        const auto& arrivalPoint = editor.getObjectById(arrivalPointId);
        if (arrivalPoint.firstCommit().value().author != uid) {
            INFO() << arrivalPointId << " WRONG AUTHOR: " << arrivalPoint.firstCommit().value().author;
            return;
        }
        if (!validateCommitTime(arrivalPoint.firstCommit().value().date)) {
            INFO() << arrivalPointId << " WRONG TIME: " << arrivalPoint.firstCommit().value().author;
            return;
        }
        if (!dryRun && !arrivalPoint.deleted()) {
            editor.deleteObject(arrivalPointId);
            INFO() << arrivalPointId << " DELETED";
        } else {
            INFO() << arrivalPointId << " TO BE DELETED";
        }
    } catch (...) {
        INFO() << arrivalPointId << " DELETE FAILED";
    }
}

void deleteArrivalPoints(
    const maps::wiki::editor_client::BasicEditorObject& company,
    maps::wiki::editor_client::Instance& editor,
    int64_t uid,
    bool dryRun)
{
    if (!company.slavesByRole().contains("assigned_arrival_point")) {
        return;
    }
    for (const auto& arrivalPoint : company.slavesByRole().at("assigned_arrival_point")) {
        deleteArrivalPoint(arrivalPoint.id, editor, uid, dryRun);
    }
}

void deleteArrivalPoints(
    const std::unordered_map<maps::wiki::poi_feed::PermalinkId, std::vector<NyakMapping>>& companiesByPermalink,
    maps::wiki::editor_client::Instance& editor,
    int64_t uid,
    bool dryRun)
{
    for (const auto& [permalink, companies] : companiesByPermalink) {
        for (const auto& company : companies) {
            if (company.dbid == 0) {
                INFO() << permalink << " NULL ID";
                continue;
            }
            INFO() << permalink << " " << company.dbid;
            deleteArrivalPoints(
                editor.getObjectById(company.dbid),
                editor,
                uid,
                dryRun);
        }
    }
}

void
saveArrivalPoints(
    std::vector<ArrivalPoint>& arrivalPoints,
    const maps::wiki::configs::editor::ConfigHolder& editorCfg,
    const std::unordered_set<std::string>& langs,
    maps::wiki::editor_client::Instance& editor,
    bool dryRun)
{
    for (auto& arrivalPoint : arrivalPoints) {
        const auto& obj = arrivalPoint.toEditorObject(editorCfg, langs);
        if (obj == std::nullopt) {
            continue;
        }
        if (dryRun) {
            INFO() << arrivalPoint.id << "TO BE SAVED";
            continue;
        }
        try {
            const auto& newObj = editor.saveObject(*obj);
            arrivalPoint.dbid = newObj.id;
            INFO() << "SAVE SUCCES: " << arrivalPoint.id << " - " << arrivalPoint.dbid;
        } catch (...) {
            INFO() << "SAVE ERROR: " << arrivalPoint.id;
        }
    }
}

std::unordered_map<maps::wiki::poi_feed::PermalinkId, std::vector<ArrivalPoint>>
processCompanyArrivalPoints(
    const std::vector<CompanySprav>& companiesSprav,
    const std::unordered_map<maps::wiki::poi_feed::PermalinkId, std::vector<NyakMapping>>& nyakMapping,
    maps::wiki::editor_client::Instance& editor,
    const maps::wiki::configs::editor::ConfigHolder& editorCfg,
    const std::unordered_set<std::string>& langs,
    bool dryRun)
{
    std::unordered_map<maps::wiki::poi_feed::PermalinkId, std::vector<ArrivalPoint>> result;
    size_t arrvalPointsCnt = 0;
    for (const auto& companySprav : companiesSprav) {
        std::vector<maps::wiki::editor_client::BasicEditorObject> masterOrgs;
        for (const auto& org : nyakMapping.at(companySprav.permalink)) {
            if (org.publishingStatus == "publish" || org.publishingStatus == "temporarily_closed") {
                if (org.dbid != 0) {
                    masterOrgs.emplace_back(editor.getObjectById(org.dbid));
                } else {
                    INFO() <<
                        "Permalink: " << companySprav.permalink <<
                        " Publishing status: " << org.publishingStatus << " ID id NULL"
                        " nyakMapping size: " << nyakMapping.at(companySprav.permalink).size();
                }
            }
        }

        std::vector<ArrivalPoint> arrivalPoints;
        for (const auto& arrivalPoint : companySprav.company.driving_arrival_points()) {
            arrivalPoints.emplace_back(ArrivalPoint(
                arrivalPoint,
                masterOrgs,
                companySprav.permalink,
                editor,
                arrvalPointsCnt));
            arrvalPointsCnt++;
        }

        saveArrivalPoints(arrivalPoints, editorCfg, langs, editor, dryRun);

        result[companySprav.permalink] = arrivalPoints;
    }
    return result;
}

// std::unordered_map<maps::wiki::poi_feed::PermalinkId, std::vector<ArrivalPoint>>
// processCompanyArrivalPoints(
//     const std::vector<CompanySprav>& companiesSprav,
//     const std::unordered_map<maps::wiki::poi_feed::PermalinkId, uint64_t>& masterOrgsIds,
//     maps::wiki::editor_client::Instance& editor,
//     const maps::wiki::configs::editor::ConfigHolder& editorCfg,
//     const std::unordered_set<std::string>& langs,
//     bool dryRun)
// {
//     std::unordered_map<maps::wiki::poi_feed::PermalinkId, std::vector<ArrivalPoint>> result;
//     size_t arrvalPointsCnt = 0;
//     for (const auto& companySprav : companiesSprav) {
//         if (!masterOrgsIds.contains(companySprav.permalink)) {
//             continue;
//         }
//         std::vector<ArrivalPoint> arrivalPoints;
//         for (const auto& arrivalPoint : companySprav.company.driving_arrival_points()) {
//             arrivalPoints.emplace_back(ArrivalPoint(
//                 arrivalPoint,
//                 {editor.getObjectById(masterOrgsIds.at(companySprav.permalink))},
//                 companySprav.permalink,
//                 editor,
//                 arrvalPointsCnt));
//             arrvalPointsCnt++;
//         }
//         saveArrivalPoints(arrivalPoints, editorCfg, langs, editor, dryRun);
//         result[companySprav.permalink] = arrivalPoints;
//     }
//     return result;
// }

std::unordered_map<maps::wiki::poi_feed::PermalinkId, std::vector<ArrivalPoint>>
processCompanyArrivalPoints(
    const maps::json::Value& companiesJson,
    maps::wiki::editor_client::Instance& editor,
    const maps::wiki::configs::editor::ConfigHolder& editorCfg,
    const std::unordered_set<std::string>& langs,
    bool dryRun)
{
    std::unordered_map<maps::wiki::poi_feed::PermalinkId, std::vector<ArrivalPoint>> result;
    for (const auto& permalink : companiesJson.fields()) {
        const auto& companyJson = companiesJson[permalink];
        std::vector<ArrivalPoint> arrivalPoints;
        for (const maps::json::Value& arrivalPoint : companyJson) {
            const auto arrivalPointObj = ArrivalPoint(arrivalPoint, editor);
            if (arrivalPointObj.dbid != 0) {
                continue;
            }
            arrivalPoints.push_back(arrivalPointObj);
        }
        if (arrivalPoints.empty()) {
            continue;
        }

        saveArrivalPoints(arrivalPoints, editorCfg, langs, editor, dryRun);

        result[stoll(permalink)] = arrivalPoints;
    }
    return result;
}

// std::string
// getCategoryIdById(
//     const uint64_t id,
//     const maps::wiki::editor_client::Instance& editor)
// {
//     const auto& obj = editor.getObjectById(id);
//     return obj.categoryId;
// }

// void
// getIdsFromJson(
//     const std::string& filepath,
//     const maps::wiki::editor_client::Instance& editor)
// {
//     INFO() << "Getting ids from json";
//     // const auto& idsJson = maps::json::Value::fromFile(filepath).as<std::vector<uint64_t>>();
//     // std::unordered_map<std::string, size_t> ids;
//     // for (const auto& id : idsJson) {
//     //     ids[getCategoryIdById(id, editor)]++;
//     // }
//     // for (const auto& [category, cnt] : ids) {
//     //     INFO() << category << " " << cnt;
//     // }

//     const auto& idsJson = maps::json::Value::fromFile(filepath);
//     ArrivalPoint ap;
//     ap.findOrgs(idsJson["81953640011"], editor);
//     return;

//     std::unordered_map<std::string, std::vector<std::string>> category_ids;
//     for (const auto& permalink : idsJson.fields()) {
//         const auto& master_ids = idsJson[permalink].as<std::vector<int64_t>>();
//         for (const auto& id : master_ids) {
//             const auto& obj = editor.getObjectById(id);
//             if (obj.deleted()) {
//                 INFO() << "Object " << id << " is deleted";
//             }
//             category_ids[permalink].push_back(obj.categoryId + (obj.deleted() ? " (deleted)" : ""));
//         }
//     }

//     maps::json::Builder b;
//     b << category_ids;
//     std::ofstream outFile("output_category_ids.json");
//     outFile << maps::json::prettifyJson(b.str()) << std::endl;
//     outFile.close();
// }

void
deleteValet(
    const maps::json::Value& companiesJson,
    maps::wiki::editor_client::Instance& editor)
    // const maps::json::Value& companiesJson)
{
    // for (const auto& [id, geos] : VALET_ARRIVAL_POINTS) {
    //     const auto& obj = editor.getObjectById(id);
    //     bool hasValet = false;
    //     if (!obj.slavesByRole().contains("assigned_arrival_point")) {
    //         INFO() << "assigned_arrival_point " << id;
    //         continue;
    //     }
    //     for (const auto& arrivalPoint : obj.slavesByRole().at("assigned_arrival_point")) {
    //         const auto& arrivalPointObj = editor.getObjectById(arrivalPoint.id);
    //         const auto& geo = arrivalPointObj.getGeometryInGeodetic().value().get<maps::geolib3::Point2>();
    //         for (const auto& valetGeo : geos) {
    //             if (geo.x() == valetGeo[0] && geo.y() == valetGeo[1]) {
    //                 try {
    //                     // editor.deleteObject(arrivalPoint.id);
    //                 } catch (...) {
    //                     INFO() << "DELETE ERROR " << id;
    //                 }
    //                 INFO() << "Valet found for " << id;
    //                 hasValet = true;
    //                 break;
    //             }
    //         }
    //     }
    //     if (!hasValet) {
    //         INFO() << "No valet found for " << id;
    //     }
    // }

    for (const auto& permalink : companiesJson.fields()) {
        const auto& companyJson = companiesJson[permalink];
        std::vector<ArrivalPoint> arrivalPoints;
        for (const maps::json::Value& arrivalPointJson : companyJson) {
            const auto& tags = arrivalPointJson["tags"].as<std::vector<std::string>>();
            const auto isValet = std::find(tags.begin(), tags.end(), "valet") != tags.end();
            const auto dbid = arrivalPointJson["dbid"].as<uint64_t>();
            if (!isValet) {
                continue;
            }
            INFO() << "Valet " << dbid;
            try {
                editor.deleteObject(dbid);
            } catch (...) {
                INFO() << "DELETE ERROR " << dbid;
            }
        }
    }
}

// std::unordered_map<maps::wiki::poi_feed::PermalinkId, NyakMapping>
// findMasters(
//     const std::vector<CompanySprav>& companiesSprav,
//     // const maps::wiki::configs::editor::ConfigHolder& editorCfg,
//     // const std::unordered_set<std::string>& langs,
//     maps::wiki::editor_client::Instance& editor,
//     const std::unique_ptr<maps::wiki::IndoorLevels>& indoorLevels)
//     // bool dryRun)
// {
//     std::unordered_map<maps::wiki::poi_feed::PermalinkId, NyakMapping> permalinksToIds = {
//         {11924246096ul, NyakMapping(6315174823ul, "publish")},
//         {130521445447ul, NyakMapping(6315174883ul, "publish")},
//         {135299512009ul, NyakMapping(6315174993ul, "publish")},
//         {143892640951ul, NyakMapping(6315174893ul, "publish")},
//         {215774590483ul, NyakMapping(6315175013ul, "publish")},
//         {63052416745ul, NyakMapping(6315174933ul, "publish")},
//         {77661305767ul, NyakMapping(6315174973ul, "publish")},
//         {86529324357ul, NyakMapping(6315174843ul, "publish")},
//         {132483106067ul, NyakMapping(6309573810ul, "publish")}
//     };
//     const std::unordered_set<maps::wiki::poi_feed::PermalinkId> indoorPermalinks = {
//         210788702467ul, 36197258641ul,  33134427195ul,  189270558976ul,
//         167681123050ul, 53044963183ul,  152388997814ul, 197275723991ul,
//         191593741576ul, 228525693258ul, 6804300863ul,   241816681614ul,
//         98876220561ul,  57884457346ul,  97454596253ul,  178566156288ul,
//         238659696072ul, 196615231305ul, 39750866927ul,  150009472923ul,
//         97111395917ul,  204453428313ul, 23283340137ul,  56215404040ul,
//         30405410593ul,  130747443597ul, 130913662225ul, 147000893187ul,
//         1201386147ul,   23269250247ul,  144148440468ul, 24820670665ul,
//         220415209033ul
//     };

//     for (const auto& companySprav : companiesSprav) {
//         if (!indoorPermalinks.contains(companySprav.permalink)) {
//             continue;
//         }
//         try {
//             const auto& mercator = maps::geolib3::convertGeodeticToMercator(maps::geolib3::Point2(
//                 companySprav.company.address().pos().point().lon(),
//                 companySprav.company.address().pos().point().lat()));
//             const auto& levelIdxs = indoorLevels->findIndoorLevels(mercator);
//             if (levelIdxs.empty()) {
//                 INFO() << "No indoor level found for " << companySprav.permalink;
//                 continue;
//             }
//             const auto& indoorPlan = editor.getObjectById(indoorLevels->level(levelIdxs.front()).indoorPlanId());
//             permalinksToIds[companySprav.permalink] = NyakMapping(
//                 editor.getObjectById(indoorPlan.mastersByRole().at("indoor_plan_assigned").front().id).id,
//                 "publish");
//         } catch (...) {
//             INFO() << "MASTER FOR INDOOR NOT FOUND " << companySprav.permalink;
//         }
//     }

//     return permalinksToIds;
// }

int main(int argc, char** argv)
{
    maps::cmdline::Parser parser;
    const auto dryRun = parser.flag("dry-run").help("poi data will not be saved");
    const auto servicesCfgPath = parser.string("services-cfg-path").help("path to local services config").required();
    const auto nmapUid = parser.string("nmap-uid").help("nmap uid").required();
    parser.parse(argc, argv);

    INFO() << (dryRun.defined() ? "DRYRUN" : "WARNING: DRYRUN DISABLED");

    const auto configDocPtr = std::make_unique<maps::wiki::common::ExtendedXmlDoc>(servicesCfgPath);
    const std::string editorUrl(configDocPtr->get<std::string>(EDITOR_URL_XPATH));
    maps::wiki::editor_client::Instance editor(editorUrl, maps::wiki::revision::UserID(std::stoll(nmapUid)));
    // maps::wiki::editor_client::Instance editor_test(
    //     "http://core-nmaps-editor.common.unstable.maps.yandex.net/",
    //     maps::wiki::revision::UserID(std::stoll(nmapUid)));
    const maps::wiki::editor_client::EditorCfg editorCfg(configDocPtr->get<std::string>(CATEGORIES_CONFIG_XPATH));
    const auto& langs = maps::wiki::tasks_sprav::supportedNmapsLangs(editorCfg);

    // deleteValet(maps::json::Value::fromFile("arrival_points_input"), editor);
    // // deleteValet(maps::json::Value::fromFile("arrival_points_input"));
    // return 0;

    // maps::wiki::common::PoolHolder poolHolder(*configDocPtr, "core", "grinder");
    // const auto levels = maps::wiki::IndoorLevels::load(poolHolder.pool(), std::nullopt);
    // deleteArrivalPoints(findMasters(companiesSprav, editor, levels), editor, std::stoll(nmapUid), dryRun.defined());
    // saveArrivalPoints(companiesSprav, editorCfg, langs, editor, levels, dryRun.defined());

    // deleteArrivalPoints(nyakMapping, editor, std::stoll(nmapUid), dryRun.defined());
    // for (const auto& id : maps::json::Value::fromFile("ids_to_delete").as<std::vector<uint64_t>>()) {
    //     try {
    //         editor.deleteObject(id);
    //         INFO() << id << " DELETED";
    //     } catch (...) {
    //         INFO() << id << " DELETE FAILED";
    //     }
    // }

    // const auto& companyArrivalPoints = processCompanyArrivalPoints(
    //     companiesSprav,
    //     nyakMapping,
    //     editor,
    //     editorCfg,
    //     langs,
    //     dryRun.defined());
    // findDublicates(companyArrivalPoints, "dublicates");
    // writeArrivalPointsJson(companyArrivalPoints, "arrival_points_output");
    // return 0;

    // const auto& companiesSprav = readCompaniesYT(COMPANIES_SPRAV_PATH);
    // const auto& nyakMapping = permalinkToDBID(NYAK_MAPPING_PATH);
    const auto& companyArrivalPoints = processCompanyArrivalPoints(
        maps::json::Value::fromFile("arrival_points_input"),
        editor, editorCfg, langs, dryRun.defined());
    findDublicates(companyArrivalPoints, "dublicates");
    writeArrivalPointsJson(companyArrivalPoints, "arrival_points_output");
    return 0;


    // const auto& companiesSprav = readCompaniesYT(COMPANIES_SPRAV_PATH);
    // INFO() << "COMPANIES_SPRAV_SIZE: " << companiesSprav.size();
    // return 0;
    // const auto& nyakMapping = permalinkToDBID(NYAK_MAPPING_PATH);
    // INFO() << "NYAK_MAPPING_SIZE: " << nyakMapping.size();

    // const auto& companyArrivalPoints = processCompanyArrivalPoints(
    //     companiesSprav,
    //     nyakMapping,
    //     editor,
    //     std::stoll(nmapUid),
    //     editorCfg,
    //     langs,
    //     dryRun.defined());
    // findDublicates(companyArrivalPoints, "dublicates");
    // writeArrivalPointsJson(companyArrivalPoints, "arrival_points_output");

    // const auto& companies = checkCompaniesArrivalPoints(companiesSprav, nyakMapping, editor);
    // writeCompaniesJson(companies, "companies_output");
    // const auto& companies = readCompaniesJson("companies_output", companiesSprav);
    // deleteArrivalPoints(companies, editor, std::stoll(nmapUid));
    // return 0;



    // auto arrivalPoints = readArrivalPointsJson("arrival_points_input", nyakMapping, editor);
    // saveArrivalPoints(arrivalPoints, editorCfg, langs, editor, dryRun.defined());
    // findDublicates(arrivalPoints, "dublicates");
    // writeArrivalPointsJson(arrivalPoints, "arrival_points_output");
}
