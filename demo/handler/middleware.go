package handler

import (
	"github.com/gin-gonic/gin"
	"net/url"
)

// GetUserInfo 从请求头中获取用户信息并将其存储在 gin.Context 中。
func GetUserInfo(ctx *gin.Context) {
	// 从请求头中获取UserName，并对其进行URL解码。
	userName, err := url.QueryUnescape(ctx.Request.Header.Get("UserName"))
	if err == nil {
		// 如果解码成功，将UserName存储在 gin.Context 中，键名为"user_name"。
		ctx.Set("user_name", userName)
	}
}
