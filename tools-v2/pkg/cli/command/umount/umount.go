/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: CurveCli
 * Created Date: 2022-06-11
 * Author: chengyi (Cyber-SiKu)
 */

package umount

import (
	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	"github.com/spf13/cobra"
)

type UmountCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*UmountCommand)(nil) // check interface

func (umountCmd *UmountCommand) AddSubCommands() {
	umountCmd.Cmd.AddCommand(
		NewFsCommand(),
	)
}

func NewUmountCommand() *cobra.Command {
	umountCmd := &UmountCommand{
		basecmd.MidCurveCmd{
			Use:   "umount",
			Short: "umount fs in the dingofs",
		},
	}
	return basecmd.NewMidCurveCli(&umountCmd.MidCurveCmd, umountCmd)
}
