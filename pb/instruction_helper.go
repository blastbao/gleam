package pb

import (
	"fmt"
)

func (m *DatasetShard) Name() string {
	return fmt.Sprintf("f%d-d%d-s%d", m.FlowHashCode, m.DatasetId, m.DatasetShardId)
}

func (m *DatasetShardLocation) Address() string {
	return fmt.Sprintf("%s:%d", m.Host, m.Port)
}

func (m *InstructionSet) InstructionNames() (stepNames []string) {
	for _, ins := range m.GetInstructions() {
		stepNames = append(stepNames, fmt.Sprintf("%d:%d", ins.StepId, ins.TaskId))
	}
	return
}

func (m *Instruction) SetInputLocations(locations []DataLocation) {
	for _, loc := range locations {
		m.InputShardLocations = append(m.InputShardLocations, &DatasetShardLocation{
			Name:   loc.Name,
			Host:   loc.Location.Server,
			Port:   int32(loc.Location.Port),
			OnDisk: loc.OnDisk,
		})
	}
}

func (m *Instruction) SetOutputLocations(locations []DataLocation) {
	for _, loc := range locations {
		m.OutputShardLocations = append(m.OutputShardLocations, &DatasetShardLocation{
			Name:   loc.Name,
			Host:   loc.Location.Server,
			Port:   int32(loc.Location.Port),
			OnDisk: loc.OnDisk,
		})
	}
}

func (m *Instruction) GetName() string {
	return fmt.Sprintf("%d:%d", m.StepId, m.TaskId)
}
