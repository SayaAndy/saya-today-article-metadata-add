package frontmatter

import (
	"bytes"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

type Metadata struct {
	Title            string    `yaml:"title"`
	ShortDescription string    `yaml:"shortDescription"`
	ActionDate       string    `yaml:"actionDate"`
	PublishedTime    time.Time `yaml:"publishedTime"`
	Thumbnail        string    `yaml:"thumbnail"`
	Tags             []string  `yaml:"tags"`
	Geolocation      string    `yaml:"geolocation"`
	Timezone         string    `yaml:"timezone"`
}

func ParseFrontmatter(content []byte) (metadata *Metadata, markdown []byte, err error) {
	if !bytes.HasPrefix(content, []byte("---\n")) {
		return nil, content, nil
	}

	end := bytes.Index(content[4:], []byte("\n---\n"))
	if end == -1 {
		return nil, content, nil
	}

	yamlContent := content[4 : end+4]
	markdownContent := content[end+9:]

	metadata = &Metadata{}
	if err := yaml.Unmarshal([]byte(yamlContent), &metadata); err != nil {
		return nil, nil, fmt.Errorf("failed to parse YAML frontmatter: %w", err)
	}

	return metadata, markdownContent, nil
}
