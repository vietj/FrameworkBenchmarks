// Code generated by easyjson for marshaling/unmarshaling. DO NOT EDIT.

package common

import (
	json "encoding/json"
	easyjson "github.com/mailru/easyjson"
	jlexer "github.com/mailru/easyjson/jlexer"
	jwriter "github.com/mailru/easyjson/jwriter"
)

// suppress unused package warning
var (
	_ *json.RawMessage
	_ *jlexer.Lexer
	_ *jwriter.Writer
	_ easyjson.Marshaler
)

func easyjsonC803d3e7DecodeCommon(in *jlexer.Lexer, out *World) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "id":
			out.Id = int32(in.Int32())
		case "randomNumber":
			out.RandomNumber = int32(in.Int32())
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjsonC803d3e7EncodeCommon(out *jwriter.Writer, in World) {
	out.RawByte('{')
	first := true
	_ = first
	{
		const prefix string = ",\"id\":"
		if first {
			first = false
			out.RawString(prefix[1:])
		} else {
			out.RawString(prefix)
		}
		out.Int32(int32(in.Id))
	}
	{
		const prefix string = ",\"randomNumber\":"
		if first {
			first = false
			out.RawString(prefix[1:])
		} else {
			out.RawString(prefix)
		}
		out.Int32(int32(in.RandomNumber))
	}
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v World) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjsonC803d3e7EncodeCommon(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v World) MarshalEasyJSON(w *jwriter.Writer) {
	easyjsonC803d3e7EncodeCommon(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *World) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjsonC803d3e7DecodeCommon(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *World) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjsonC803d3e7DecodeCommon(l, v)
}
func easyjsonC803d3e7DecodeCommon1(in *jlexer.Lexer, out *JSONResponse) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "message":
			out.Message = string(in.String())
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjsonC803d3e7EncodeCommon1(out *jwriter.Writer, in JSONResponse) {
	out.RawByte('{')
	first := true
	_ = first
	{
		const prefix string = ",\"message\":"
		if first {
			first = false
			out.RawString(prefix[1:])
		} else {
			out.RawString(prefix)
		}
		out.String(string(in.Message))
	}
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v JSONResponse) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjsonC803d3e7EncodeCommon1(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v JSONResponse) MarshalEasyJSON(w *jwriter.Writer) {
	easyjsonC803d3e7EncodeCommon1(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *JSONResponse) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjsonC803d3e7DecodeCommon1(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *JSONResponse) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjsonC803d3e7DecodeCommon1(l, v)
}
