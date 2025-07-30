package frontmatter

import (
	"fmt"
	"regexp"
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
}

func ParseFrontmatter(content []byte) (metadata *Metadata, markdown []byte, err error) {
	frontmatterRegex := regexp.MustCompile(`^---\s*\r?\n([\s\S]*?)\r?\n---\s*\r?\n([\s\S]*)$`)
	matches := frontmatterRegex.FindSubmatch(content)

	if len(matches) != 3 {
		return nil, content, nil
	}

	yamlContent := matches[1]
	markdownContent := matches[2]

	metadata = &Metadata{}
	if err := yaml.Unmarshal([]byte(yamlContent), &metadata); err != nil {
		return nil, nil, fmt.Errorf("failed to parse YAML frontmatter: %w", err)
	}

	return metadata, markdownContent, nil
}
