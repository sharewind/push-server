package templates

func init() {
	registerTemplate("channel.html", `

<div class="row-fluid"><div class="span6">
{{if .Channel}}
<table class="table table-striped table-condensed table-bordered">
    {{$c := .Channel}}
    <tr>
        <td>id</td>
        <td>{{$c.ID}}</td>
    </tr>
    <tr>
        <td>name</td>
        <td>{{$c.Name}}</td>
    </tr>
    <tr>
        <td>create_at</td>
        <td>{{$c.CreatedAt}}</td>
    </tr>
    <tr>
        <td>creator</td>
        <td>{{$c.Creator}}</td>
    </tr>
    <tr>
        <td>appid</td>
        <td>{{$c.APPID}}</td>
    </tr>
    <tr>
        <td>appname</td>
        <td>{{$c.APPName}}</td>
    </tr>
    <tr>
        <td>messagecount</td>
        <td>{{$c.MessageCount}}</td>
    </tr>
</table>
{{else}}
<div class="alert"><h4>Notice</h4>No Topics Found</div>
{{end}}

<table class="table table-striped table-condensed table-bordered">
    messages
    <tr>
        <td>id</td>
        <td>body</td>
        <td>create_at</td>
        <td>create_by</td>
        <td>creator_id</td>
        <td>expires</td>
        <td>device_type</td>
        <td>push_type</td>
        <td>channel_id</td>
        <td>device_id</td>
        <td>ok</td>
        <td>err</td>
    </tr>
    {{range $m := .Messages}}
    <tr>
        <td>{{$m.ID}}</td>
        <td>{{$m.Body}}</td>
        <td>{{$m.CreatedAt}}</td>
        <td>{{$m.CreatedBy}}</td>
        <td>{{$m.CreatorID}}</td>
        <td>{{$m.Expires}}</td>
        <td>{{$m.DeviceType}}</td>
        <td>{{$m.PushType}}</td>
        <td>{{$m.ChannelID}}</td>
        <td>{{$m.DeviceID}}</td>
        <td>{{$m.OK}}</td>
        <td>{{$m.Err}}</td>
    </tr>
    {{end}}
</table>

<table class="table table-striped table-condensed table-bordered">
    subscribes
    <tr>
        <td>ChannelID</td>
        <td>DeviceID</td>
        <td>DeviceType</td>
        <td>CreatedAt</td>
        <td>UpdatedAt</td>
    </tr>
    {{range $s := .Subscribes}}
    <tr>
        <td>{{$s.ChannelID}}</td>
        <td>{{$s.DeviceID}}</td>
        <td>{{$s.DeviceType}}</td>
        <td>{{$s.CreatedAt}}</td>
        <td>{{$s.UpdatedAt}}</td>
    </tr>
    {{end}}
</table>
<table class="table table-striped table-condensed table-bordered">
    sub count
    {{range $key, $value := .SubCount}}
    <tr>
        <td>{{$key}}</td>
        <td>{{$value}}</td>
    </tr>
    {{end}}
</table>
</div></div>
{{template "js.html" .}}
`)
}
