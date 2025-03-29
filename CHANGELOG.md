# Changelog

All notable changes to this project will be documented in this file.

## [0.8.1] - 2025-03-29
### Details
#### Changed
- Update opentelemetry stack to the latest v0.28 (dev dependencies only) by @alex-karpenko in [#36](https://github.com/alex-karpenko/sacs/pull/36)
- Update dev dependencies by @alex-karpenko in [#38](https://github.com/alex-karpenko/sacs/pull/38)

#### Fixed
- Increase MSRV to 1.81 by @alex-karpenko in [#37](https://github.com/alex-karpenko/sacs/pull/37)

## [0.8.0] - 2024-12-19
### Details
#### Breaking changes
- Use cron-lite crate instead of cron to reduce dependencies by @alex-karpenko in [#34](https://github.com/alex-karpenko/sacs/pull/34)

#### Changed
- Improve unit tests by @alex-karpenko in [#33](https://github.com/alex-karpenko/sacs/pull/33)

#### Fixed
- Get rid of deprecated tracing method in tests by @alex-karpenko in [#31](https://github.com/alex-karpenko/sacs/pull/31)

## [0.7.0] - 2024-11-22
### Details
#### Breaking changes
- Update dependencies, cron v0.13 and thiserror v2.0 by @alex-karpenko

#### Changed

## [0.6.4] - 2024-10-22
### Details
#### Changed
- Update dev tracing dependencies to the latest versions by @alex-karpenko in [#28](https://github.com/alex-karpenko/sacs/pull/28)

#### Fixed
- Fix lint warnings by @alex-karpenko in [#29](https://github.com/alex-karpenko/sacs/pull/29)

## [0.6.3] - 2024-09-06
### Details
#### Changed
- Improve tests quality by @alex-karpenko in [#26](https://github.com/alex-karpenko/sacs/pull/26)

#### Fixed
- Fix lint warning in documentation comments by @alex-karpenko in [#27](https://github.com/alex-karpenko/sacs/pull/27)

## [0.6.2] - 2024-07-27
### Details
#### Changed
- Update dev dependencies by @alex-karpenko in [#25](https://github.com/alex-karpenko/sacs/pull/25)

## [0.6.1] - 2024-06-14
### Details
#### Fixed
- Update traces to use debug level for spans by @alex-karpenko in [#23](https://github.com/alex-karpenko/sacs/pull/23)

## [0.6.0] - 2024-06-10
### Details
#### Added
- Implement Task with restricted execution time by @alex-karpenko in [#18](https://github.com/alex-karpenko/sacs/pull/18)
- Implement well-know traits for public API by @alex-karpenko in [#22](https://github.com/alex-karpenko/sacs/pull/22)

#### Changed
- Improve Task state tracking and statistics by @alex-karpenko in [#19](https://github.com/alex-karpenko/sacs/pull/19)

#### Fixed
- Update documentation by @alex-karpenko in [#21](https://github.com/alex-karpenko/sacs/pull/21)

## [0.5.1] - 2024-06-03
### Details
#### Added
- Add auto changelog generation to release workflows by @alex-karpenko in [#14](https://github.com/alex-karpenko/sacs/pull/14)
- Add basic tracing by @alex-karpenko in [#16](https://github.com/alex-karpenko/sacs/pull/16)

## [0.5.0] - 2024-05-07
### Details
#### Changed
- Merge latest v0.4 changes to main by @alex-karpenko in [#8](https://github.com/alex-karpenko/sacs/pull/8)
- Change internal ID representation from Uuid to arbitrary String by @alex-karpenko in [#9](https://github.com/alex-karpenko/sacs/pull/9)
- Improve unit tests by @alex-karpenko in [#13](https://github.com/alex-karpenko/sacs/pull/13)

#### Fixed
- Rename codecov workflow file by @alex-karpenko
- Change error on scheduling duplicate to DuplicatedTaskId by @alex-karpenko in [#11](https://github.com/alex-karpenko/sacs/pull/11)
- Update Readme with links to documentation by @alex-karpenko
- Prevent manual instantiation of JobId by @alex-karpenko in [#12](https://github.com/alex-karpenko/sacs/pull/12)

## [0.4.2] - 2024-05-05
### Details
#### Changed
- Make task clonnable Implement Task::with_id and Task::with_schedule methods by @alex-karpenko in [#7](https://github.com/alex-karpenko/sacs/pull/7)

#### Fixed
- Force check for duplicated TaskId during scheduling by @alex-karpenko in [#6](https://github.com/alex-karpenko/sacs/pull/6)

## [0.4.1] - 2024-04-29
### Details
#### Added
- Implement Task::new_with_id method to create task with predefined Id by @Artem468 in [#5](https://github.com/alex-karpenko/sacs/pull/5)

#### Fixed
- Update readme with shields, add publish workflow by @alex-karpenko

## New Contributors
* @Artem468 made their first contribution in [#5](https://github.com/alex-karpenko/sacs/pull/5)

## [0.4.0] - 2024-04-21
### Details
#### Added
- Implement immediate garbage collector by @alex-karpenko in [#4](https://github.com/alex-karpenko/sacs/pull/4)

#### Fixed
- Update deny config by @alex-karpenko
- Normalize Cargo.toml by @alex-karpenko

## [0.3.0] - 2024-04-07
### Details
#### Changed
- Make delay configurable for IntervalDelayed tasks by @alex-karpenko in [#3](https://github.com/alex-karpenko/sacs/pull/3)

## [0.2.1] - 2024-03-03
### Details
#### Fixed
- Bug with task cancellation by @alex-karpenko

## [0.2.0] - 2024-03-03
### Details
#### Added
- Implement first draft v0.1 by @alex-karpenko in [#1](https://github.com/alex-karpenko/sacs/pull/1)

#### Fixed
- Update crate config by @alex-karpenko
- Update crate config by @alex-karpenko
- Rename scheduler drop method to avoid conflicts by @alex-karpenko in [#2](https://github.com/alex-karpenko/sacs/pull/2)

[0.8.1]: https://github.com/alex-karpenko/sacs/compare/v0.8.0..v0.8.1
[0.8.0]: https://github.com/alex-karpenko/sacs/compare/v0.7.0..v0.8.0
[0.7.0]: https://github.com/alex-karpenko/sacs/compare/v0.6.4..v0.7.0
[0.6.4]: https://github.com/alex-karpenko/sacs/compare/v0.6.3..v0.6.4
[0.6.3]: https://github.com/alex-karpenko/sacs/compare/v0.6.2..v0.6.3
[0.6.2]: https://github.com/alex-karpenko/sacs/compare/v0.6.1..v0.6.2
[0.6.1]: https://github.com/alex-karpenko/sacs/compare/v0.6.0..v0.6.1
[0.6.0]: https://github.com/alex-karpenko/sacs/compare/v0.5.1..v0.6.0
[0.5.1]: https://github.com/alex-karpenko/sacs/compare/v0.5.0..v0.5.1
[0.5.0]: https://github.com/alex-karpenko/sacs/compare/v0.4.2..v0.5.0
[0.4.2]: https://github.com/alex-karpenko/sacs/compare/v0.4.1..v0.4.2
[0.4.1]: https://github.com/alex-karpenko/sacs/compare/v0.4.0..v0.4.1
[0.4.0]: https://github.com/alex-karpenko/sacs/compare/v0.3.0..v0.4.0
[0.3.0]: https://github.com/alex-karpenko/sacs/compare/v0.2.1..v0.3.0
[0.2.1]: https://github.com/alex-karpenko/sacs/compare/v0.2.0..v0.2.1
[0.2.0]: https://github.com/alex-karpenko/sacs/compare/v0.0.0..v0.2.0

<!-- generated by git-cliff -->
