$script = @@#py
def get_permalinks(filename):
    filename_utf8 = filename.decode('utf-8')
    permalinks = set()
    with open(filename_utf8, "r") as f:
        for line in f:
            permalink = line.strip().encode('utf-8')
            permalinks.add(permalink)
    return permalinks
@@;

$get_permalinks = Python3::get_permalinks(Callable<(string)->set<string>>, $script);
$permalinks = $get_permalinks(FilePath("^^FILE^^"));

INSERT INTO hahn.`tmp/^^USER^^/permalinks` WITH TRUNCATE
SELECT permalink, export_proto
FROM hahn.`home/altay/db/export/current-state/snapshot/company` VIEW raw
WHERE CAST(permalink AS STRING) in $permalinks;
