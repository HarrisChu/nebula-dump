package schemacache

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
	"gopkg.in/yaml.v3"
)

type (
	Schemacache interface {
		Update() error
		ListSpaces() []int32
		GetSpace(space int32) *meta.SpaceItem
		GetTags(space int32) []*meta.TagItem
		GetEdges(space int32) []*meta.EdgeItem
		GetIndexes(space int32) []*meta.IndexItem
		Close() error
	}

	FileCache struct {
		path    string
		name    string
		spaces  map[int32]*meta.SpaceItem
		tags    map[int32][]*meta.TagItem
		edges   map[int32][]*meta.EdgeItem
		indexes map[int32][]*meta.IndexItem
		client  *common.MetaClient
		rwMutex sync.RWMutex
		address string
	}

	CacheData struct {
		LastUpdateTime string       `yaml:"last_update_time,omitempty"`
		Spaces         []*SpaceData `yaml:"spaces,omitempty"`
	}

	SpaceData struct {
		Id      int32        `yaml:"id,omitempty"`
		Space   string       `yaml:"space,omitempty"`
		Tags    []*TagData   `yaml:"tags,omitempty"`
		Edges   []*EdgeData  `yaml:"edges,omitempty"`
		Indexes []*IndexData `yaml:"indexes,omitempty"`
	}

	TagData struct {
		Tag string `yaml:"tag,omitempty"`
	}
	EdgeData struct {
		Edge string `yaml:"edge,omitempty"`
	}
	IndexData struct {
		Index string `yaml:"index,omitempty"`
	}
)

var _ Schemacache = &FileCache{}

const (
	defaultPath = ".meta_cache"
	defaultName = "cache.yaml"
)

func NewFileCache(address string) (Schemacache, error) {

	c := &FileCache{}
	client, err := common.NewMetaClient(address)
	if err != nil {
		return nil, err
	}
	c.client = client
	h, err := os.UserHomeDir()
	if err != nil {
		c.path = defaultPath
	} else {
		c.path = filepath.Join(h, defaultPath)
	}
	c.name = defaultName
	c.address = address
	c.spaces = make(map[int32]*meta.SpaceItem)
	c.tags = make(map[int32][]*meta.TagItem)
	c.edges = make(map[int32][]*meta.EdgeItem)
	c.indexes = make(map[int32][]*meta.IndexItem)
	return c, nil

}

func (c *FileCache) Close() error {
	if c.client != nil {
		c.client.Close()
	}
	return nil
}

func (c *FileCache) Update() error {
	// get the last update time from meta.
	// if the time is same, return directly

	// update and flush schema
	if err := c.updateSchema(); err != nil {
		return err
	}

	if err := c.writeToFile(); err != nil {
		return err
	}
	return nil
}

func (c *FileCache) updateSchema() error {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	req := meta.NewListSpacesReq()
	resp, err := c.client.Client.ListSpaces(req)
	if err != nil {
		return err
	}
	for _, space := range resp.GetSpaces() {
		id := *space.GetId().SpaceID
		spaceReq := meta.NewGetSpaceReq().SetSpaceName(space.GetName())
		spaceResp, err := c.client.Client.GetSpace(spaceReq)
		if err != nil {
			return err
		}
		c.spaces[id] = spaceResp.GetItem()

		tagReq := meta.NewListTagsReq().SetSpaceID(id)
		tagResp, err := c.client.Client.ListTags(tagReq)
		if err != nil {
			return err
		}
		c.tags[id] = tagResp.Tags

		edgeReq := meta.NewListEdgesReq().SetSpaceID(id)
		edgeResp, err := c.client.Client.ListEdges(edgeReq)
		if err != nil {
			return err
		}
		c.edges[id] = edgeResp.Edges

		indexTagReq := meta.NewListTagIndexesReq().SetSpaceID(id)
		indexTagResp, err := c.client.Client.ListTagIndexes(indexTagReq)
		if err != nil {
			return err
		}

		indexEdgeReq := meta.NewListEdgeIndexesReq().SetSpaceID(id)
		indexEdgeResp, err := c.client.Client.ListEdgeIndexes(indexEdgeReq)
		if err != nil {
			return err
		}
		c.indexes[id] = indexTagResp.Items
		c.indexes[id] = append(c.indexes[id], indexEdgeResp.Items...)
	}
	return nil
}

func (c *FileCache) ListSpaces() []int32 {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()
	d := make([]int32, 0)
	for id := range c.spaces {
		d = append(d, id)
	}
	return d
}

func (c *FileCache) GetSpace(space int32) *meta.SpaceItem {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()
	d, ok := c.spaces[space]
	if ok {
		return d
	}
	return nil
}
func (c *FileCache) GetTags(space int32) []*meta.TagItem {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()
	d, ok := c.tags[space]
	if ok {
		return d
	}
	return nil

}
func (c *FileCache) GetEdges(space int32) []*meta.EdgeItem {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()
	d, ok := c.edges[space]
	if ok {
		return d
	}
	return nil
}
func (c *FileCache) GetIndexes(space int32) []*meta.IndexItem {
	d, ok := c.indexes[space]
	if ok {
		return d
	}
	return nil
}

func (c *FileCache) convertToData() (*CacheData, error) {
	d := &CacheData{
		Spaces: make([]*SpaceData, 0),
	}

	for id, space := range c.spaces {
		spaceData := &SpaceData{
			Id:      id,
			Tags:    make([]*TagData, 0),
			Edges:   make([]*EdgeData, 0),
			Indexes: make([]*IndexData, 0),
		}
		var s []byte
		if err := common.CompactSerializer(space, &s); err != nil {
			return nil, err
		}
		spaceData.Space = string(s)

		for _, tag := range c.tags[id] {
			var d []byte
			if err := common.CompactSerializer(tag, &d); err != nil {
				return nil, err
			}
			tagData := &TagData{Tag: string(d)}
			spaceData.Tags = append(spaceData.Tags, tagData)
		}

		for _, edge := range c.edges[id] {
			var d []byte
			if err := common.CompactSerializer(edge, &d); err != nil {
				return nil, err
			}
			edgeData := &EdgeData{Edge: string(d)}
			spaceData.Edges = append(spaceData.Edges, edgeData)
		}

		for _, index := range c.indexes[id] {
			var d []byte
			if err := common.CompactSerializer(index, &d); err != nil {
				return nil, err
			}
			indexData := &IndexData{Index: string(d)}
			spaceData.Indexes = append(spaceData.Indexes, indexData)
		}
		d.Spaces = append(d.Spaces, spaceData)
	}
	return d, nil

}

func (c *FileCache) convertFromData(d *CacheData) error {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	for _, spaceData := range d.Spaces {
		id := spaceData.Id
		var space meta.SpaceItem
		d := []byte(spaceData.Space)
		err := common.CompactDeserializer(&space, &d)
		if err != nil {
			return err
		}
		c.spaces[id] = &space
		for _, tagData := range spaceData.Tags {
			d := []byte(tagData.Tag)
			var tag meta.TagItem
			err := common.CompactDeserializer(&tag, &d)
			if err != nil {
				return err
			}
			c.tags[id] = append(c.tags[id], &tag)
		}
		for _, edgeData := range spaceData.Edges {
			d := []byte(edgeData.Edge)
			var edge meta.EdgeItem
			err := common.CompactDeserializer(&edge, &d)
			if err != nil {
				return err
			}
			c.edges[id] = append(c.edges[id], &edge)
		}
		for _, indexData := range spaceData.Indexes {
			d := []byte(indexData.Index)
			var index meta.IndexItem
			err := common.CompactDeserializer(&index, &d)
			if err != nil {
				return err
			}
			c.indexes[id] = append(c.indexes[id], &index)
		}
	}
	return nil

}

func (c *FileCache) readFromFile() error {
	file := filepath.Join(c.path, c.address, c.name)
	if _, err := os.Stat(file); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	in, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	var d CacheData
	if err := yaml.Unmarshal(in, &d); err != nil {
		return err
	}
	if err := c.convertFromData(&d); err != nil {
		return err
	}
	return nil
}

func (c *FileCache) writeToFile() error {
	file := filepath.Join(c.path, c.address, c.name)
	dir := filepath.Dir(file)
	if _, err := os.Stat(dir); err != nil {
		if err := os.MkdirAll(dir, os.ModeDir|os.ModePerm); err != nil {
			return err
		}
	}
	d, err := c.convertToData()
	if err != nil {
		return err
	}
	out, err := yaml.Marshal(&d)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(file, out, 0644)
	if err != nil {
		return err
	}
	return nil
}
