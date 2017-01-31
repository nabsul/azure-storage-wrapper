/**
 * For the sake of developing this thing quickly, I opted NOT to mock azure-storage.
 * The "unit tests" therefore must run against a real azure storage account.
 * Simply copy this file to `test/config.js` and fill in credentials accordingly
 */

module.exports = {
	credentials: '', // credentials to access an azure storage account
};
