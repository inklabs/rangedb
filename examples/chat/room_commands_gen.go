// Code generated by go generate; DO NOT EDIT.
// This file was generated at
// 2021-01-30 11:27:48.039104 -0800 PST m=+0.001821984
package chat

func (c OnBoardRoom) AggregateID() string   { return c.RoomID }
func (c OnBoardRoom) AggregateType() string { return "room" }
func (c OnBoardRoom) CommandType() string   { return "OnBoardRoom" }

func (c JoinRoom) AggregateID() string   { return c.RoomID }
func (c JoinRoom) AggregateType() string { return "room" }
func (c JoinRoom) CommandType() string   { return "JoinRoom" }

func (c SendMessageToRoom) AggregateID() string   { return c.RoomID }
func (c SendMessageToRoom) AggregateType() string { return "room" }
func (c SendMessageToRoom) CommandType() string   { return "SendMessageToRoom" }

func (c SendPrivateMessageToRoom) AggregateID() string   { return c.RoomID }
func (c SendPrivateMessageToRoom) AggregateType() string { return "room" }
func (c SendPrivateMessageToRoom) CommandType() string   { return "SendPrivateMessageToRoom" }

func (c RemoveUserFromRoom) AggregateID() string   { return c.RoomID }
func (c RemoveUserFromRoom) AggregateType() string { return "room" }
func (c RemoveUserFromRoom) CommandType() string   { return "RemoveUserFromRoom" }

func (c BanUserFromRoom) AggregateID() string   { return c.RoomID }
func (c BanUserFromRoom) AggregateType() string { return "room" }
func (c BanUserFromRoom) CommandType() string   { return "BanUserFromRoom" }
