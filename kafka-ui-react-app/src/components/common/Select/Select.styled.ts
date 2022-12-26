import styled from 'styled-components';

interface Props {
  selectSize: 'M' | 'L';
  isLive?: boolean;
  minWidth?: string;
  disabled?: boolean;
  isThemeMode?: boolean;
}

interface OptionProps {
  disabled?: boolean;
}

export const Select = styled.ul<Props>`
  position: relative;
  list-style: none;
  display: flex;
  gap: ${(props) => (props.isLive ? '5px' : '0')};
  align-items: center;
  height: ${(props) => (props.selectSize === 'M' ? '32px' : '40px')};
  border: 1px
    ${({ theme, disabled, isThemeMode }) => {
      if (isThemeMode) {
        return 'none';
      }
      if (disabled) {
        return theme.select.borderColor.disabled;
      }

      return theme.select.borderColor.normal;
    }}
    solid;
  border-radius: 4px;
  font-size: 14px;
  width: fit-content;
  padding-left: 16px;
  padding-right: 16px;
  color: ${({ theme, disabled }) =>
    disabled ? theme.select.color.disabled : theme.select.color.normal};
  min-width: ${({ minWidth }) => minWidth || 'auto'};
  cursor: ${({ disabled }) => (disabled ? 'not-allowed' : 'pointer')};
  &:hover {
    color: ${({ theme, disabled }) =>
      disabled ? theme.select.color.disabled : theme.select.color.hover};
    border-color: ${({ theme, disabled }) =>
      disabled
        ? theme.select.borderColor.disabled
        : theme.select.borderColor.hover};
  }
  &:focus {
    outline: none;
    color: ${({ theme }) => theme.select.color.active};
    border-color: ${({ theme }) => theme.select.borderColor.active};
  }
  &:disabled {
    color: ${({ theme }) => theme.select.color.disabled};
    border-color: ${({ theme }) => theme.select.borderColor.disabled};
  }
`;

export const OptionList = styled.ul`
  position: absolute;
  top: 100%;
  left: 0;
  max-height: 228px;
  margin-top: 4px;
  background-color: ${({ theme }) => theme.select.backgroundColor.normal};
  border: 1px ${({ theme }) => theme.select.borderColor.normal} solid;
  border-radius: 4px;
  font-size: 14px;
  line-height: 18px;
  color: ${({ theme }) => theme.select.color.normal};
  overflow-y: auto;
  z-index: 10;
  max-width: 300px;
  min-width: 100%;
  align-items: center;
  & div {
    white-space: nowrap;
  }
  &::-webkit-scrollbar {
    -webkit-appearance: none;
    width: 7px;
  }

  &::-webkit-scrollbar-thumb {
    border-radius: 4px;
    background-color: ${({ theme }) =>
      theme.select.optionList.scrollbar.backgroundColor};
  }

  &::-webkit-scrollbar:horizontal {
    height: 7px;
  }
`;

export const Option = styled.li<OptionProps>`
  display: flex;
  list-style: none;
  padding: 10px 12px;
  transition: all 0.2s ease-in-out;
  cursor: ${({ disabled }) => (disabled ? 'not-allowed' : 'pointer')};
  gap: 5px;
  color: ${({ theme, disabled }) =>
    theme.select.color[disabled ? 'disabled' : 'normal']};

  &:hover {
    background-color: ${({ theme, disabled }) =>
      theme.select.backgroundColor[disabled ? 'normal' : 'hover']};
  }

  &:active {
    background-color: ${({ theme }) => theme.select.backgroundColor.active};
  }
`;

export const SelectedOption = styled.li`
  padding-right: 16px;
  list-style-position: inside;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  & svg {
    path {
      fill: ${({ theme }) => theme.defaultIconColor};
    }
  }
  & div {
    display: none;
  }
`;
