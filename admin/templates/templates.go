package templates

import (
	"github.com/op/go-logging"
	"html/template"
)

var log = logging.MustGetLogger("template")
var sources map[string]string
var T *template.Template

func init() {
	T = template.New("_top_")
}

func registerTemplate(name string, source string) {
	if sources == nil {
		sources = make(map[string]string)
	}
	_, ok := sources[name]
	if ok {
		log.Fatalf("ERROR: duplicate template %s", name)
	}
	sources[name] = source
}

func Parse() {
	for name, source := range sources {
		_, err := T.New(name).Parse(source)
		if err != nil {
			log.Fatalf("ERROR: failed to parse template %s - %s", name, err)
		}
	}
}
