from pyspark import SparkContext


sc = SparkContext(appName="Consolidate Data", master="local")

rdd = sc.wholeTextFiles("/data")


def parse_document(file_tuple):
    filepath, content = file_tuple
    filename = filepath.split('/')[-1].replace('.txt', '')
    parts = filename.split('_', 1)
    doc_id = parts[0]
    doc_title = parts[1].replace('_', ' ') if len(parts) > 1 else filename
    clean_content = content.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
    clean_title = doc_title.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
    return "{}\t{}\t{}".format(doc_id, clean_title, clean_content)


output_rdd = rdd.map(parse_document)
output_rdd.coalesce(1).saveAsTextFile("/input/data")

sc.stop()
