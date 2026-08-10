const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');

// The docker plugin reads docker-compose.yaml, substitutes ${config.*} from the *instance* native config
// and creates every service whose iobEnabled label ends up truthy. An instance upgraded from 3.x has no
// dockerMysql/dockerPhpMyAdmin in its native config at all, so the ":-" defaults decide what happens - and
// they must never opt anybody into containers they did not ask for (issue #524).
describe('docker-compose.yaml', function () {
    const composeFile = path.join(__dirname, '..', 'docker-compose.yaml');
    const yaml = fs.readFileSync(composeFile, 'utf-8');

    // The plugin exposes only its index, so the parser is loaded by path. If that internal layout ever
    // changes, the tests using it are skipped instead of failing - the label check below still guards the file.
    function loadPluginInternals() {
        try {
            const root = path.dirname(require.resolve('@iobroker/plugin-docker/package.json'));
            const lib = path.join(root, 'build', 'cjs', 'lib');
            const parseDockerCompose = require(path.join(lib, 'parseDockerCompose.js'));
            const { composeToContainerConfigs } = require(path.join(lib, 'compose2config.js'));
            const { walkTheConfig } = require(path.join(lib, 'templates.js'));
            return {
                parse: parseDockerCompose.default || parseDockerCompose,
                composeToContainerConfigs,
                walkTheConfig,
            };
        } catch {
            return null;
        }
    }

    /** Render the compose file with the given instance native config and return the resulting services */
    function renderWith(native) {
        const plugin = loadPluginInternals();
        if (!plugin) {
            return null;
        }
        const rendered = plugin.walkTheConfig(plugin.parse(yaml), native, { instance: 0 });
        return plugin.composeToContainerConfigs(rendered);
    }

    it('every iobEnabled label defaults to false', function () {
        const labels = yaml.match(/iobEnabled=\S+?'/g) || [];
        assert.ok(labels.length >= 2, `expected iobEnabled labels, found ${labels.length}`);
        for (const label of labels) {
            assert.ok(
                label.includes(':-false}'),
                `${label} must use ":-false" as default, otherwise instances without that config key get the container created`,
            );
        }
    });

    it('no container is enabled when the instance config has no docker settings', function () {
        // native of an instance that was upgraded from 3.x: no dockerMysql / dockerPhpMyAdmin
        const configs = renderWith({ dbtype: 'mysql', host: 'localhost', dbname: 'iobroker' });
        if (!configs) {
            return this.skip();
        }

        assert.ok(configs.length, 'expected the compose file to declare services');
        for (const config of configs) {
            assert.strictEqual(
                !!config.iobEnabled,
                false,
                `service "${config.name}" would be created although docker was never enabled`,
            );
        }
    });

    it('no container is enabled with the defaults from io-package.json', function () {
        const configs = renderWith(JSON.parse(JSON.stringify(require('../io-package.json').native)));
        if (!configs) {
            return this.skip();
        }

        for (const config of configs) {
            assert.strictEqual(!!config.iobEnabled, false, `service "${config.name}" is enabled by default`);
        }
    });

    it('the containers are created when docker is enabled in the instance config', function () {
        const configs = renderWith({
            dockerMysql: { enabled: true },
            dockerPhpMyAdmin: { enabled: true },
        });
        if (!configs) {
            return this.skip();
        }

        assert.strictEqual(
            configs.filter(config => config.iobEnabled).length,
            configs.length,
            'all services must be enabled when the user enabled docker',
        );
    });
});
