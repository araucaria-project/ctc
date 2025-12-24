# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]
### Added
### Changed
### Fixed

## 1.1.15 - 2025-09-03
### Fixed
- `cycle_time_calc` dither was on where shouldn't be
- `cycle_time_calc` add possible commands _available_param_telesc_commands from USE_OBJECT_PARAMS_IN
- `cycle_time_calc` for commands are in USE_OBJECT_PARAMS_IN and below minimum exp no - intercept is not added

## 1.1.6 - 2025-09-02
### Changed
- Typing fix, add some logging

## 1.1.5 - 2025-02-20
### Added
- Add error serve to json

## 1.1.4 - 2025-01-08
### Added
- Now commands can change readout mode.


## 1.0.19 - 2025-01-06
### Changed
- Wait sunrise sunset sec changed.


## 1.0.18 - 2024-11-27
### Changed
- Now DARK, SNAP and ZERO use OBJECT params


## 1.0.15 - 2024-10-10
### Changed
- Changing for asyncio (all ok, only calculator still sync)
- Add option to train once per day
- Add aiofiles as async file reader / writer


## 1.0.1 - 2024-09-21
### Added
- Tpg calculation method option  with average dome and mount distance


## 1.0.0 - 2024-09-20
### Added
- First production release
- Documentation
- readout modes input


### Changed
- Changed some methods for private


## 0.1.0 - 2024-09-19
### Added
- Project core files added and initialized.
