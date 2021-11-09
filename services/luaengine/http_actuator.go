package luaengine

import (
	"github.com/Hamster601/Budd/pkg/logs"
	lua "github.com/yuin/gopher-lua"
)

func httpModule(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _httpModuleApi)
	L.Push(t)
	return 1
}

var _httpModuleApi = map[string]lua.LGFunction{
	"get":    doGet,
	"delete": doDelete,
	"post":   doPost,
	"put":    doPut,
}

func doGet(L *lua.LState) int {
	ret := L.NewTable()
	paramUrl := L.CheckString(1)
	paramOps := L.CheckTable(2)

	cli := _httpClient.GET(paramUrl)
	if headers, ok := lvToMap(paramOps); ok {
		cli.SetHeaders(headers)
	}

	entity, err := cli.DoForEntity()
	if err != nil {
		logs.Error(err.Error())
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	ret.RawSet(lua.LString("status_code"), lua.LNumber(entity.StatusCode()))
	ret.RawSet(lua.LString("body"), lua.LString(string(entity.Data())))

	L.Push(ret)
	return 1
}

func doDelete(L *lua.LState) int {
	ret := L.NewTable()
	paramUrl := L.CheckString(1)
	paramOps := L.CheckTable(2)

	cli := _httpClient.DELETE(paramUrl)
	if headers, ok := lvToMap(paramOps); ok {
		cli.SetHeaders(headers)
	}

	entity, err := cli.DoForEntity()
	if err != nil {
		logs.Error(err.Error())
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	ret.RawSet(lua.LString("status_code"), lua.LNumber(entity.StatusCode()))
	ret.RawSet(lua.LString("body"), lua.LString(string(entity.Data())))

	L.Push(ret)
	return 1
}

func doPost(L *lua.LState) int {
	ret := L.NewTable()
	paramUrl := L.CheckString(1)
	paramHeaders := L.CheckTable(2)
	paramContents := L.CheckTable(3)

	cli := _httpClient.POST(paramUrl)
	if headers, ok := lvToMap(paramHeaders); ok {
		cli.SetHeaders(headers)
	}

	contents, ok := lvToMap(paramContents)
	if !ok {
		logs.Error("The argument must Table")
		L.Push(lua.LNil)
		L.Push(lua.LString("The argument must Table"))
		return 2
	}

	entity, err := cli.SetBodyAsForm(contents).DoForEntity()
	if err != nil {
		logs.Error(err.Error())
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	ret.RawSet(lua.LString("status_code"), lua.LNumber(entity.StatusCode()))
	ret.RawSet(lua.LString("body"), lua.LString(string(entity.Data())))

	L.Push(ret)
	return 1
}

func doPut(L *lua.LState) int {
	ret := L.NewTable()
	paramUrl := L.CheckString(1)
	paramHeaders := L.CheckTable(2)
	paramContents := L.CheckTable(3)

	cli := _httpClient.PUT(paramUrl)
	if headers, ok := lvToMap(paramHeaders); ok {
		cli.SetHeaders(headers)
	}

	contents, ok := lvToMap(paramContents)
	if !ok {
		logs.Error("The argument must Table")
		L.Push(lua.LNil)
		L.Push(lua.LString("The argument must Table"))
		return 2
	}

	entity, err := cli.SetBodyAsForm(contents).DoForEntity()
	if err != nil {
		logs.Error(err.Error())
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	ret.RawSet(lua.LString("status_code"), lua.LNumber(entity.StatusCode()))
	ret.RawSet(lua.LString("body"), lua.LString(string(entity.Data())))

	L.Push(ret)
	return 1
}
