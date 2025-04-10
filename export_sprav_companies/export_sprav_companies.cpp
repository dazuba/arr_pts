#include <maps/libs/cmdline/include/cmdline.h>
#include <maps/libs/json/include/builder.h>
#include <maps/libs/json/include/prettify.h>
#include <maps/libs/json/include/std.h>
#include <maps/libs/json/include/value.h>
#include <maps/libs/log8/include/log8.h>
#include <maps/wikimap/mapspro/libs/common/include/default_config.h>
#include <maps/wikimap/mapspro/libs/common/include/extended_xml_doc.h>
#include <maps/wikimap/mapspro/libs/common/include/pgpool3_helpers.h>
#include <maps/wikimap/mapspro/libs/common/include/yt.h>
#include <maps/wikimap/mapspro/libs/configs_editor/include/config_holder.h>
#include <maps/wikimap/mapspro/libs/editor_client/include/exception.h>
#include <maps/wikimap/mapspro/libs/configs_editor/include/externals.h>
#include <maps/wikimap/mapspro/libs/editor_client/include/object_helpers.h>
#include <maps/wikimap/mapspro/services/tasks_sprav/src/libs/common/include/name_utils.h>
#include <maps/wikimap/mapspro/services/tasks_sprav/src/libs/common/include/sprav_export.h>
#include <maps/wikimap/mapspro/services/tasks_sprav/src/libs/indoor_levels/include/indoor_level.h>
#include <maps/wikimap/mapspro/services/tasks_sprav/src/merge_poi_worker/names.h>

const TString CLUSTER = "hahn";
const std::string POI_CATEGORY_PREFIX = "poi_";
const std::string EDITOR_URL_XPATH = "/config/services/editor/url";
const std::string CATEGORIES_CONFIG_XPATH = "/config/services/editor/config";
const size_t MAX_SECONDARY_RUBRICS = 2;
const double PERMALINK_SEARCH_RADIUS = 300.0; // meters

using namespace maps::wiki;

std::vector<poi_feed::FeedObjectData>
readSpravPoiData(
    const std::unordered_set<std::string>& supportedNmapsLangs,
    const std::string& path,
    std::map<std::string, std::vector<uint64_t>>& badPermalinks)
{
    INFO() << "poiData reading started";

    auto client = common::yt::createYtClient(CLUSTER);
    std::vector<poi_feed::FeedObjectData> result;
    for (auto reader = client->CreateTableReader<NYT::TNode>(TString(path)); reader->IsValid(); reader->Next()) {
        const NYT::TNode& row = reader->GetRow();
        const auto permalink = row["permalink"].IntCast<poi_feed::PermalinkId>();
        if (row["export_proto"].IsNull()) {
            badPermalinks["empty_export_proto"].push_back(permalink);
            continue;
        }
        NSpravExport::TExportedCompany exportProto;
        Y_PROTOBUF_SUPPRESS_NODISCARD exportProto.ParseFromString(row["export_proto"].AsString());
        result.emplace_back(
            tasks_sprav::createFeedObjectData(
                exportProto,
                {},
                permalink,
                0,
                supportedNmapsLangs));
    }

    INFO() << "poiData reading finished";
    return result;
}

std::string
selectPoiCategory(const std::set<std::string>& categoryIds)
{
    const auto it = std::find_if(
        categoryIds.begin(),
        categoryIds.end(),
        [&](const auto id) {
            return id.starts_with(POI_CATEGORY_PREFIX);
        });
    if (it == categoryIds.end()) {
        return {};
    }
    return *it;
}

std::set<poi_feed::RubricId>
selectRubrics(const std::set<poi_feed::RubricId>& rubrics)
{
    if (rubrics.size() <= MAX_SECONDARY_RUBRICS) {
        return rubrics;
    }
    return std::set<poi_feed::RubricId>(rubrics.begin(), std::next(rubrics.begin(), MAX_SECONDARY_RUBRICS));
}

std::string
findPoiCategory(
    const configs::editor::FtTypeId& ftTypeId,
    const configs::editor::ConfigHolder& editorCfg)
{
    const auto categoryIds = editor_client::findCategoryIds(ftTypeId, editorCfg);
    const auto it = std::find_if(
        categoryIds.begin(),
        categoryIds.end(),
        [&](const auto id) {
            return id.starts_with(POI_CATEGORY_PREFIX);
        });
    if (it == categoryIds.end()) {
        return {};
    }
    return *it;
}

std::optional<editor_client::BasicEditorObject>
prepareCreatePoiData(
    const poi_feed::FeedObjectData& objectData,
    const configs::editor::ConfigHolder& editorCfg,
    std::string& error)
{
    if (!objectData.position()) {
        error = "empty_position_error";
        return {};
    }
    if (!objectData.rubricId()) {
        error = "empty_rubricId_error";
        return {};
    }
    const auto rubricId = *objectData.rubricId();
    const auto& rubrics = editorCfg.externals().rubrics();
    const auto newDispClass = rubrics.defaultDispClass(rubricId);
    const auto ftTypeId = rubrics.defaultFtType(rubricId);
    if (!ftTypeId) {
        error = "ft_type_error";
        return {};
    }
    const auto poiCategory = findPoiCategory(*ftTypeId, editorCfg);
    if (poiCategory.empty()) {
        error = "category_error";
        return {};
    }

    auto newObject = editor_client::createWithDefaultAttributes(
        poiCategory,
        editorCfg);
    newObject.setGeometryInGeodetic(
        maps::geolib3::Point2(
            objectData.position()->lon,
            objectData.position()->lat));
    editor_client::setPositionQuality(newObject, std::string(), editorCfg);
    editor_client::setFtTypeId(newObject, *ftTypeId, editorCfg);
    editor_client::setRubricId(newObject, rubricId, editorCfg);
    editor_client::setSecondaryRubricsIds(newObject, selectRubrics(objectData.secondaryRubricIds()));
    if (objectData.permalink()) {
        editor_client::setPermalink(newObject, objectData.permalink(), editorCfg);
    }
    if (newDispClass) {
        editor_client::setDispClass(newObject, *newDispClass, editorCfg);
    }

    merge_poi::Names names(objectData);
    names.writeTo(newObject, editorCfg);

    return newObject;
}

template <class T>
void outputJson(
    const T& t,
    const std::string& fileName)
{
    maps::json::Builder builder;
    builder << t;

    std::ofstream outFile(fileName);
    outFile << maps::json::prettifyJson(builder.str()) << std::endl;
    outFile.close();
}

int main(int argc, char** argv)
{
    maps::cmdline::Parser parser;
    const auto dryRun = parser.flag("dry-run").help("poi data will not be saved");
    const auto companiesYtPath = parser.string("companies-yt-path").help("path to companies on YT").required();
    const auto nmapUid = parser.string("nmap-uid").help("nmap uid").required();
    const auto servicesCfgPath = parser.string("services-cfg-path").help("path to local services config");
    parser.parse(argc, argv);

    const auto configDocPtr = servicesCfgPath.defined()
        ? std::make_unique<common::ExtendedXmlDoc>(servicesCfgPath)
        : common::loadDefaultConfig();

    const std::string editorUrl(configDocPtr->get<std::string>(EDITOR_URL_XPATH));
    editor_client::Instance editor(editorUrl, revision::UserID(std::stoll(nmapUid)));
    const configs::editor::ConfigHolder editorCfg(configDocPtr->get<std::string>(CATEGORIES_CONFIG_XPATH));
    const auto langs = tasks_sprav::supportedNmapsLangs(editorCfg);
    common::PoolHolder poolHolder(*configDocPtr, "core", "grinder");
    const auto levels = IndoorLevels::load(poolHolder.pool(), std::nullopt);

    std::map<std::string, std::vector<uint64_t>> badPermalinks;
    std::map<std::string, uint64_t> permalinksToNmapId;
    const auto poiData = readSpravPoiData(langs, companiesYtPath, badPermalinks);
    INFO() << "poiData saving started";

    for (auto& poi : poiData) {
        const auto mercator = poi.positionMercator();
        if (mercator.has_value() && !levels->findIndoorLevels(mercator.value()).empty()) {
            badPermalinks["indoor_org_error"].push_back(poi.permalink());
            continue;
        }
        if (!editor.getObjectsByBusinessId(
                std::to_string(poi.permalink()),
                poi.positionMercator(),
                PERMALINK_SEARCH_RADIUS).empty()) {
            badPermalinks["existing_org_error"].push_back(poi.permalink());
            continue;
        }

        std::string error;
        auto saveData = prepareCreatePoiData(
            poi,
            editorCfg,
            error);
        if (!error.empty()) {
            badPermalinks[error].push_back(poi.permalink());
            continue;
        }

        if (dryRun.defined()) {
            continue;
        }
        try {
            auto object = editor.saveObject(*saveData);
            permalinksToNmapId[std::to_string(poi.permalink())] = object.id;
        } catch (const editor_client::ServerException& e) {
            badPermalinks["server_exceptions"].push_back(poi.permalink());
        } catch (const std::exception& e) {
            badPermalinks["unknown_exceptions"].push_back(poi.permalink());
        }
    }

    INFO() << "poiData saving finished";
    outputJson(badPermalinks, "bad_permalinks.json");
    outputJson(permalinksToNmapId, "permalinks_to_nmap.json");

    return 0;
}
