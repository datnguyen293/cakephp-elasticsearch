<?php

App::uses('ModelBehavior', 'Model');

require ROOT . '/vendor/autoload.php';

class ElasticsearchBehavior extends ModelBehavior {

    public $clientPool = array();
    public $client = null;

    /**
     * This function will be called automatically when this behavior is attached to a model.
     * We don't need to initialize the ElasticSearch client when this Behavior
     * is setup, because we won't need until used, and it can be slightly expensive
     */
    public function setup(Model $Model, $settings = array()) {
        parent::setup($Model, $settings);

        Configure::load('elasticsearch');
    }

    /**
     * When we need ElasticSearch client, call this function to initialize
     * @return Elasticsearch client
     */
    public function setupClient(Model $Model) {
        if (array_key_exists($Model->alias, $this->clientPool)) {
            // already setup - reuse
            $this->client = $this->clientPool[$Model->alias];
            return $this->client;
        }

        $settings = array(
            'hosts' => Configure::read('Elasticsearch.hosts')
        );

        // Initialize Elasticsearch client
        $this->client =  new Elasticsearch\Client($settings);
        if (!is_object($this->client)) {
            die('The ElasticSearchIndexableBehavior requires the Elasticsearch-php vendor');
        }

        $this->clientPool[$Model->alias] = $this->client;

        return $this->client;
    }

    /**
     * Standard afterSave() callback, which will be triggered after a record is saved
     */
    /*public function afterSave(Model $Model, $created, $options = array()) {
        $this->saveDataToIndex($Model, $Model->id);

        return parent::afterSave($Model, $created, $options);
    }*/

    /**
     * Use this function to manually delete an index from ElasticSearch
     */
    public function deleteIndexByModelId(Model $Model, $id) {
        $params = array(
            'index' => Configure::read('Elasticsearch.index'),
            'type' => strtolower(Inflector::pluralize($Model->alias)),
            'id'    => $id,
            'ignore' => [404, 503]
        );

        $client = $this->setupClient($Model);
        $doc = $client->get($params);
        $doc = $this->formatESResult($doc);

        if (isset($doc['found']) && $doc['found']) {
            unset($params['ignore']);
            return $client->delete($params);
        }
        return false;
    }

    /**
     * Use this function to manually update an index by record ID
     *  @param Model $Model
     *  @param $id - Record id in database
     */
    public function saveDataToIndex(Model $Model, $id, $data) {
        // $data = $Model->findById($id);

        if (!empty($data)) {
            // Create location pin
            /*if (!empty($data[$Model->alias]['latitude'])) {
                $data[$Model->alias]['location'] = array(
                    'lat' => $data[$Model->alias]['latitude'],
                    'lon' => $data[$Model->alias]['longitude']
                );
            }*/


            $params = array(
                'index' => Configure::read('Elasticsearch.index'),
                'type' => strtolower(Inflector::pluralize($Model->alias)),
                'id'    => $id,
                'ignore' => [404, 503]
            );

            $client = $this->setupClient($Model);
            $doc = $client->get($params);
            $doc = $this->formatESResult($doc);

            $deleteIndex = false;

            // If model has [active_ind] field and which is set to 0, then delete the related index
            if (empty($data[$Model->alias]['active_ind'])) {
                $deleteIndex = true;
            }

            if (!empty($doc['found'])) {
                if ($deleteIndex) {
                    try {
                        $params = array(
                            'index' => Configure::read('Elasticsearch.index'),
                            'type' => strtolower(Inflector::pluralize($Model->alias)),
                            'id' => $Model->id
                        );
                        $client->delete($params);
                    }
                    catch (Exception $e) {
                    }
                }
                else {
                    unset($params['ignore']);
                    $params['body']['doc'] = $data[$Model->alias];
                    $client->update($params);
                }
            }
            else {
                if (!$deleteIndex) {
                    unset($params['ignore']);
                    $params['body'] = $data[$Model->alias];
                    $client->index($params);
                }
            }
        }
    }

    /**
     * Search restaurants by keyword over fields
     *  @param $fieldNames - Array('field1', 'field2', ...) or just a single field 'fieldName'
     *  @param $keyword
     *
     *  Examples:
     *      $response = $this->Restaurant->searchByTextFields(array('name', 'address1'), 'Pizza');
     *      $response = $this->Restaurant->searchByTextFields('name', 'Pizza');
     */
    public function searchByTextFields(Model $Model, $fieldNames = array(), $keyword, $offset = 0, $limit = 10) {
        $params = array(
            'index' => Configure::read('Elasticsearch.index'),
            'type' => strtolower(Inflector::pluralize($Model->alias))
        );

        if (is_string($fieldNames)) {
            $params['body']['query'] = array(
                'fuzzy_like_this_field' => array(
                    $fieldNames => array(
                        'like_text' => $keyword,
                        'max_query_terms' => 12
                    )
                )
            );
            // $params['body']['query']['match'][$fieldNames] = $keyword;
        }
        else if (is_array($fieldNames)) {
            $params['body']['query']['multi_match'] = array(
                'query' => $keyword,
                'fields' => $fieldNames
            );
        }

        $params['body']['from'] = $offset;
        $params['body']['size'] = $limit;

        $response = $this->setupClient($Model)->search($params);
        return $response['hits']['hits'];
    }

    /**
     * Search by text fields and filter by location field
     *
     *  @param $fields = array('field1', 'field2',...) or just a single field 'fieldName'
     *  @param $keyword - Keyword to search, empty allowed
     *  @param $center = array('lat' => $lat, 'lon' => $lon)
     *  @param $distance - (number) Distance from $center location (miles)
     *
     *  Examples:
     *      $response = $this->Restaurant->searchWithDistance(array('name', 'business_info'), 'Pizza', array('lat' => 38.837543, 'lon' => -77.434149), 0.1);
     *      $response = $this->Restaurant->searchWithDistance(null, null, array('lat' => 38.837543, 'lon' => -77.434149), 0.1);
     *
     */
    public function searchWithDistance(Model $Model, $fieldNames = array(), $keyword = null, $center = null, $distance = 10, $offset = 0, $limit = 20, $random = true) {
        $params = array(
            'index' => Configure::read('Elasticsearch.index'),
            'type' => strtolower(Inflector::pluralize($Model->alias))
        );

        // Match text fields
        if (!empty($keyword)) {
            if (is_string($fieldNames)) {
                $params['body']['query']['filtered']['query']['match'][$fieldNames] = $keyword;
            }
            else if (is_array($fieldNames)) {
                $params['body']['query']['filtered']['query']['multi_match'] = array(
                    'query' => $keyword,
                    'fields' => $fieldNames
                );
            }
        }

        // Filter by location
        if (is_array($center) && isset($center['lat']) && isset($center['lon'])) {
            $params['body']['query']['filtered']['filter'] = array(
                'geo_distance' => array(
                    'distance' => ((double)$distance . 'mi'),
                    'location' => $center
                )
            );
        }

        // Random order
        if ($random) {
            $params['body']['query'] = array('function_score' => array('query' => $params['body']['query']));

            $randomScore = new \stdClass();
            
            App::uses('CakeSession', 'Model/Datasource');
            $seed = CakeSession::read('random_seed');
            if (empty($seed)) {
                $seed = rand(1, 1000);
                CakeSession::write('random_seed', $seed);
            }
            $randomScore->seed = $seed;

            $params['body']['query']['function_score']['random_score'] = $randomScore;
            $params['body']['query']['function_score']['boost_mode'] = 'replace';
        }

        // Limit and offset
        $params['body']['from'] = $offset;
        $params['body']['size'] = $limit;

        $response = $this->setupClient($Model)->search($params);

        return $response['hits']['hits'];
    }

    /**
     * This function takes a parameter $query in Elasticsearch format for advanced search
     *  @param array $query
     *  @return array - List of hits
     *
     *  Examples:
     *      $response = $this->Restaurant->advancedSearch(array());
     */
    public function advancedSearch(Model $Model, $query = array()) {
        if (!isset($query['query'])) {
            $query = array('query' => $query);
        }


        $params = array(
            'index' => Configure::read('Elasticsearch.index'),
            'type' => strtolower(Inflector::pluralize($Model->alias))
        );
        $params['body'] = $query;
        $response = $this->setupClient($Model)->search($params);
        return $response['hits']['hits'];
    }

    /**
     * This function takes a parameter $query in Elasticsearch format for count resutls
     *   @param array $query
     *   @return array - count of results
     */
    public function countSearchResults(Model $Model, $query = array()) {
     if (!isset($query['query'])) {
         $query = array('query' => $query);
     }


     $params = array(
         'index' => Configure::read('Elasticsearch.index'),
         'type' => strtolower(Inflector::pluralize($Model->alias))
     );
     $params['body'] = $query;
     $response = $this->setupClient($Model)->count($params);
     return $response['count'];
    }

    /**
     * Get document by id
     */
    public function getIndexById(Model $Model, $id) {
        $params = array(
            'index' => Configure::read('Elasticsearch.index'),
            'type' => strtolower(Inflector::pluralize($Model->alias)),
            'id' => $id,
            'ignore' => [404, 503]
        );
        $doc = $this->setupClient($Model)->get($params);
        $doc = $this->formatESResult($doc);

        if (isset($doc['found']) && $doc['found']) {
            return array($Model->alias => $doc['_source']);
        }
        return null;
    }

    /**
     * Convert ElasticSearch result to array()
     */
    private function formatESResult($result) {
        if (is_string($result)) {
            $result = json_decode($result, true);
        }
        else if (is_object($result)) {
            $result = (array)$result;
        }

        return $result;
    }

}
