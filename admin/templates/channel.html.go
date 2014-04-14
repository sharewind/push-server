package templates

func init() {
	registerTemplate("channel.html", `

<div class="row-fluid"><div class="span6">
{{if .Channel}}
<table class="table table-condensed table-bordered">
    {{$c := .Channel}}
    <tr>
        <td>id:{{$c.ID}}</td>
    </tr>
    <tr>
        <td>name:{{$c.Name}}</td>
    </tr>
    <tr>
        <td>create_at:{{$c.CreatedAt}}</td>
    </tr>
    <tr>
        <td>creator:{{$c.Creator}}</td>
    </tr>
    <tr>
        <td>appid:{{$c.APPID}}</td>
    </tr>
    <tr>
        <td>appname:{{$c.APPName}}</td>
    </tr>
    <tr>
        <td>messagecount:{{$c.MessageCount}}</td>
    </tr>
</table>
{{else}}
<div class="alert"><h4>Notice</h4>No Topics Found</div>
{{end}}

<table>
    {{range $m := .Messages}}
    <tr>
        <td>message:{{$m}}</td>
    </tr>
    {{end}}
</table>

<table>
    {{range $s := .Subscribes}}
    <tr>
        <td>subscribe:{{$s}}</td>
    </tr>
    {{end}}
</table>
<table>
    {{range $key, $value := .SubCount}}
    <tr>
        <td>{{$key}}:{{$value}}</td>
    </tr>
    {{end}}
</table>
</div></div>
{{template "js.html" .}}
`)
}
