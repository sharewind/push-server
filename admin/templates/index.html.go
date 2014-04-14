package templates

func init() {
	registerTemplate("index.html", `

<div class="row-fluid"><div class="span12">
<h2>push</h2>
</div></div>


<div class="row-fluid"><div class="span6">
{{if .Channels}}
<table class="table table-condensed table-bordered">
{{range $c := .Channels}}
    <tr>
        <td><a href="/channel/{{.ID}}">{{.Name}}</a></td>
    </tr>
{{end}}
</table>
{{else}}
<div class="alert"><h4>Notice</h4>No Topics Found</div>
{{end}}

</div></div>
{{template "js.html" .}}
`)
}
